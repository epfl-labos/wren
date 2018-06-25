#include "rpc/async_rpc_client.h"
#include "common/utils.h"
#include "common/sys_logger.h"
#include <unistd.h>

namespace scc {

    AsyncRPCClient::AsyncRPCClient(std::string host, int port, int numChannels)
            : _numChannels(numChannels) {

        srand(Utils::GetThreadId());

        for (int i = 0; i < _numChannels; i++) {
            // create message channels
            MessageChannelPtr mc(new MessageChannel(host, port));
            _msgChannels.push_back(mc);

            // initialize data structures
            PendingCallMapPtr s(new PendingCallMap());
            _pendingCallSets.push_back(s);
            MutexPtr m(new std::mutex());
            _pendingCallSetMutexes.push_back(m);

            WaitHandle *readyEvent = new WaitHandle();
            _replyHandlingThreadReadyEvents.push_back(readyEvent);
        }


        // signal all threads ready
        _allReplyHandlingThreadsReady.Set();
    }

    AsyncRPCClient::~AsyncRPCClient() {
        for (unsigned int i = 0; i < _replyHandlingThreadReadyEvents.size(); i++) {
            delete _replyHandlingThreadReadyEvents[i];
        }
    }

    void AsyncRPCClient::Call(RPCMethod rid, std::string &args, AsyncState &state) {
        PbRpcRequest request;

        int channelId = random() % _numChannels;

        int64_t messageId = GetMessageId();
        request.set_msgid(messageId);
        request.set_methodid(static_cast<int32_t> (rid));
        request.set_arguments(args);

        // add call state to the pending list
        {
            std::lock_guard<std::mutex> lk(*_pendingCallSetMutexes[channelId]);
            _pendingCallSets[channelId]->insert(PendingCallMap::value_type(messageId, &state));
        }

        // send request
        _msgChannels[channelId]->Send(request);
    }

    void AsyncRPCClient::CallWithNoState(RPCMethod rid, std::string &args) {
        try {
            PbRpcRequest request;

            int64_t messageId = GetMessageId(); //TODO: do we need a mesg id if we do not wait for a reply?
            int channelId = messageId % _numChannels;
            request.set_msgid(messageId);
            request.set_methodid(static_cast<int32_t> (rid));
            request.set_arguments(args);
            std::thread::id this_id = std::this_thread::get_id();

//       send request
            _msgChannels[channelId]->Send(request);
        }
        catch (SocketException &e) {
            fprintf(stdout, "[ERROR]: AsyncRPCClient::CallWithNoState: SocketException: %s\n", e.what());
            exit(1);
        }
        catch (...) {
            fprintf(stdout, "SEVERE AsyncRPCClient::CallWithNoState socket exception: %s\n", "...");
            fflush(stdout);
        }
    }



    void AsyncRPCClient::HandleReplyMessage(int channelId) {
        MessageChannelPtr msgChannel(_msgChannels[channelId]);
        PendingCallMapPtr _pendingCallSet = _pendingCallSets[channelId];
        MutexPtr _pendingCallSetMutex = _pendingCallSetMutexes[channelId];

        // I'm ready
        _replyHandlingThreadReadyEvents[channelId]->Set();

        // wait the others ready
        _allReplyHandlingThreadsReady.Wait();

        try {
            while (true) {
                // exit if the associated object is not alive anymore
                if (msgChannel.use_count() == 1) {
                    return;
                }
                // receive reply
                PbRpcReply reply;
                std::thread::id this_id = std::this_thread::get_id();
                msgChannel->Recv(reply);

                // get and remove the call state
                AsyncState *state = NULL;

                // notify call finished
                state->Results = reply.results();
                state->FinishEvent.Set();
            }
        }
        catch (SocketException &e) {
            fprintf(stdout, "[ERROR]:AsyncRPCClient::HandleReplyMessage: SocketException: %s\n", e.what());
            exit(1);
        }
        catch (...) {
            fprintf(stdout, "SEVERE AsyncRPCClient::HandleReplyMessage socket exception: %s\n", "...");
            fflush(stdout);
        }
    }

} // namespace scc

