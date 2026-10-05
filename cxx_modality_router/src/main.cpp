#include <arpa/inet.h>
#include <endian.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <zmq.h>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

namespace {

std::atomic<bool> g_running{true};

enum class Modality : uint8_t { Text = 0, Audio = 1, Video = 2 };

struct __attribute__((packed)) WireHeader {
    uint8_t modality;
    uint64_t session_id;
    uint64_t pts_us;
};

struct FrameMeta {
    uint64_t session_id;
    uint64_t pts_us;
};

struct MediaFrame {
    Modality modality;
    FrameMeta meta;
    std::shared_ptr<uint8_t[]> buffer;
    size_t total_length;
    size_t payload_offset;
};

struct RouterConfig {
    std::string ingest_host;
    int ingest_port;
    std::string operator_host;
    int operator_port;
    std::string ipc_endpoint;
    int send_hwm;
    size_t max_frame_bytes;
};

struct OperatorLink {
    int fd;
    sockaddr_in address;
};

struct ZmqBus {
    void* context;
    void* publisher;
};

void handle_signal(int) {
    g_running.store(false);
}

void install_signal_handlers() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
}

std::string parse_config_path(int argc, char** argv) {
    const std::string prefix = "--config=";
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg.rfind(prefix, 0) == 0) {
            return arg.substr(prefix.size());
        }
    }
    return "../config/edge_node_lan.json";
}

std::optional<RouterConfig> load_config(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) {
        return std::nullopt;
    }
    nlohmann::json doc = nlohmann::json::parse(file, nullptr, false);
    if (doc.is_discarded()) {
        return std::nullopt;
    }
    RouterConfig cfg;
    cfg.ingest_host = doc.value("ingest_host", "127.0.0.1");
    cfg.ingest_port = doc.value("ingest_port", 7400);
    cfg.operator_host = doc.value("operator_host", "127.0.0.1");
    cfg.operator_port = doc.value("operator_port", 7500);
    cfg.ipc_endpoint = doc.value("ipc_endpoint", "ipc:///tmp/alioth_frames");
    cfg.send_hwm = doc.value("send_hwm", 256);
    cfg.max_frame_bytes = doc.value("max_frame_bytes", static_cast<size_t>(1 << 20));
    return cfg;
}

sockaddr_in make_address(const std::string& host, int port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    return addr;
}

int open_ingest_socket(const RouterConfig& cfg) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return -1;
    }
    int rcvbuf = 8 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    timeval timeout{0, 100000};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in addr = make_address(cfg.ingest_host, cfg.ingest_port);
    if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

std::optional<OperatorLink> open_operator_link(const RouterConfig& cfg) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }
    int sndbuf = 8 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    return OperatorLink{fd, make_address(cfg.operator_host, cfg.operator_port)};
}

std::optional<ZmqBus> open_zmq_bus(const RouterConfig& cfg) {
    void* context = zmq_ctx_new();
    if (!context) {
        return std::nullopt;
    }
    void* publisher = zmq_socket(context, ZMQ_PUB);
    if (!publisher) {
        zmq_ctx_term(context);
        return std::nullopt;
    }
    int linger = 0;
    zmq_setsockopt(publisher, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_setsockopt(publisher, ZMQ_SNDHWM, &cfg.send_hwm, sizeof(cfg.send_hwm));
    if (zmq_bind(publisher, cfg.ipc_endpoint.c_str()) != 0) {
        zmq_close(publisher);
        zmq_ctx_term(context);
        return std::nullopt;
    }
    return ZmqBus{context, publisher};
}

void close_zmq_bus(ZmqBus& bus) {
    zmq_close(bus.publisher);
    zmq_ctx_term(bus.context);
}

std::shared_ptr<uint8_t[]> allocate_frame_buffer(size_t capacity) {
    return std::shared_ptr<uint8_t[]>(new uint8_t[capacity]);
}

std::optional<Modality> decode_modality(uint8_t raw) {
    if (raw > static_cast<uint8_t>(Modality::Video)) {
        return std::nullopt;
    }
    return static_cast<Modality>(raw);
}

std::optional<MediaFrame> parse_frame(std::shared_ptr<uint8_t[]> buffer, size_t received) {
    if (received <= sizeof(WireHeader)) {
        return std::nullopt;
    }
    WireHeader header;
    std::memcpy(&header, buffer.get(), sizeof(header));
    auto modality = decode_modality(header.modality);
    if (!modality) {
        return std::nullopt;
    }
    MediaFrame frame;
    frame.modality = *modality;
    frame.meta = FrameMeta{be64toh(header.session_id), be64toh(header.pts_us)};
    frame.buffer = std::move(buffer);
    frame.total_length = received;
    frame.payload_offset = sizeof(WireHeader);
    return frame;
}

std::optional<MediaFrame> receive_frame(int ingest_fd, size_t max_frame_bytes) {
    auto buffer = allocate_frame_buffer(max_frame_bytes);
    ssize_t received = recv(ingest_fd, buffer.get(), max_frame_bytes, 0);
    if (received <= 0) {
        return std::nullopt;
    }
    return parse_frame(std::move(buffer), static_cast<size_t>(received));
}

const char* topic_for(Modality modality) {
    switch (modality) {
        case Modality::Text:
            return "text";
        case Modality::Audio:
            return "audio";
        case Modality::Video:
            return "video";
    }
    return "unknown";
}

bool dispatch_to_operator_p2p(const OperatorLink& link, const MediaFrame& frame) {
    ssize_t sent = sendto(link.fd, frame.buffer.get(), frame.total_length, 0,
                          reinterpret_cast<const sockaddr*>(&link.address),
                          sizeof(link.address));
    return sent == static_cast<ssize_t>(frame.total_length);
}

void release_frame_reference(void*, void* hint) {
    delete static_cast<std::shared_ptr<uint8_t[]>*>(hint);
}

bool send_topic(void* socket, Modality modality) {
    const char* topic = topic_for(modality);
    return zmq_send(socket, topic, std::strlen(topic), ZMQ_SNDMORE) >= 0;
}

bool send_meta(void* socket, const FrameMeta& meta) {
    return zmq_send(socket, &meta, sizeof(meta), ZMQ_SNDMORE) >= 0;
}

bool send_payload_zero_copy(void* socket, const MediaFrame& frame) {
    auto* holder = new std::shared_ptr<uint8_t[]>(frame.buffer);
    zmq_msg_t message;
    int rc = zmq_msg_init_data(&message, frame.buffer.get() + frame.payload_offset,
                               frame.total_length - frame.payload_offset,
                               release_frame_reference, holder);
    if (rc != 0) {
        delete holder;
        return false;
    }
    if (zmq_msg_send(&message, socket, ZMQ_DONTWAIT) < 0) {
        zmq_msg_close(&message);
        return false;
    }
    return true;
}

bool dispatch_to_zeromq_ipc(const ZmqBus& bus, const MediaFrame& frame) {
    return send_topic(bus.publisher, frame.modality) &&
           send_meta(bus.publisher, frame.meta) &&
           send_payload_zero_copy(bus.publisher, frame);
}

void route_frame(const OperatorLink& link, const ZmqBus& bus, const MediaFrame& frame) {
    if (!dispatch_to_operator_p2p(link, frame)) {
        std::fprintf(stderr, "operator dispatch failed session=%llu\n",
                     static_cast<unsigned long long>(frame.meta.session_id));
    }
    if (!dispatch_to_zeromq_ipc(bus, frame)) {
        std::fprintf(stderr, "ipc dispatch failed session=%llu\n",
                     static_cast<unsigned long long>(frame.meta.session_id));
    }
}

void run_router_loop(int ingest_fd, const OperatorLink& link, const ZmqBus& bus,
                     size_t max_frame_bytes) {
    while (g_running.load()) {
        auto frame = receive_frame(ingest_fd, max_frame_bytes);
        if (frame) {
            route_frame(link, bus, *frame);
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    install_signal_handlers();

    auto config = load_config(parse_config_path(argc, argv));
    if (!config) {
        std::fprintf(stderr, "failed to load config\n");
        return 1;
    }

    int ingest_fd = open_ingest_socket(*config);
    if (ingest_fd < 0) {
        std::fprintf(stderr, "failed to bind ingest socket\n");
        return 1;
    }

    auto link = open_operator_link(*config);
    if (!link) {
        close(ingest_fd);
        std::fprintf(stderr, "failed to open operator link\n");
        return 1;
    }

    auto bus = open_zmq_bus(*config);
    if (!bus) {
        close(link->fd);
        close(ingest_fd);
        std::fprintf(stderr, "failed to open zeromq bus\n");
        return 1;
    }

    run_router_loop(ingest_fd, *link, *bus, config->max_frame_bytes);

    close_zmq_bus(*bus);
    close(link->fd);
    close(ingest_fd);
    return 0;
}
