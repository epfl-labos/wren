#include "rpc/message_channel.h"

namespace scc {

  MessageChannel::MessageChannel(std::string host, unsigned short port)
  : _host(host),
  _port(port),
  _socket(NULL) {
    // create a socket and connect to the remote server
    _socket = new TCPSocket(host, port);
  }

  // A socket should not be used in multiple message channels.

  MessageChannel::MessageChannel(TCPSocket* socket)
  : _host(),
  _port(-1),
  _socket(socket) { }

  MessageChannel::~MessageChannel() {
    delete _socket;
  }

}
