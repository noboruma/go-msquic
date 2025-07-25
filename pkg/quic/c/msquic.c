#include <stdlib.h>
#include <stdint.h>
#define QUIC_API_ENABLE_PREVIEW_FEATURES 1
#include "inc/msquic.h"

#define UNREFERENCED_PARAMETER(P) (void)(P)

#ifndef UNREFERENCED_PARAMETER
#define UNREFERENCED_PARAMETER(P) (void)(P)
#endif

#define LOGS_ENABLED 0

#include "utils.c"

// Go bindings
extern void newConnectionCallback(HQUIC, HQUIC);
extern void newStreamCallback(HQUIC, HQUIC);
extern uint32_t newReadCallback(HQUIC, HQUIC, const QUIC_BUFFER*, uint32_t len);
extern void closeConnectionCallback(HQUIC);
extern void closePeerConnectionCallback(HQUIC);
extern void closeStreamCallback(HQUIC,HQUIC);
extern void startStreamCallback(HQUIC,HQUIC);
extern void startConnectionCallback(HQUIC);
extern void freeSendBuffer(uint8_t *);
extern void newDatagramCallback(HQUIC, const QUIC_BUFFER*);
extern void abortStreamCallback(HQUIC,HQUIC);
extern void shutConnectionCallback(HQUIC);
extern void peerAddressChangedCallback(HQUIC);

HQUIC Registration = NULL;
const QUIC_API_TABLE* MsQuic = NULL;

struct QUICConfig {
	int DisableCertificateValidation;
	int MaxBidiStreams;
	int IdleTimeoutMs;
	int KeepAliveMs;
	char * keyFile;
	char * certFile;
	int MaxBindingStatelessOperations;
	int MaxStatelessOperations;
	QUIC_BUFFER Alpn;
	int EnableDatagramReceive;
	int DisableSendBuffering;
	int MaxBytesPerKey;
};

int64_t
StreamWrite(
	_In_ HQUIC Connection,
	_In_ HQUIC Stream,
	_In_ uint8_t *array,
	_In_ int64_t len,
	_In_ uint8_t noAlloc

)
{
	QUIC_BUFFER* SendBuffer = NULL;
	if (noAlloc != 1) {
		char * SendBufferRaw = malloc(sizeof(QUIC_BUFFER) + len);
		if (SendBufferRaw == NULL) {
			printf("SendBufferRaw allocation failed!\n");
			return -1;
		}
		SendBuffer = (QUIC_BUFFER*)SendBufferRaw;
		memcpy(SendBufferRaw+sizeof(QUIC_BUFFER), array, len);
		SendBuffer->Buffer = (uint8_t*)SendBufferRaw + sizeof(QUIC_BUFFER);
		SendBuffer->Length = len;
	} else {
		SendBuffer = malloc(sizeof(QUIC_BUFFER));
		if (SendBuffer == NULL) {
			printf("SendBuffer allocation failed!\n");
			return -1;
		}
		SendBuffer->Buffer = array;
		SendBuffer->Length = len;
	}

	QUIC_STATUS Status;
	if (QUIC_FAILED(Status = MsQuic->StreamSend(Stream, SendBuffer, 1, QUIC_SEND_FLAG_NONE, SendBuffer))) {
		free(SendBuffer);
		return -1;
	}
	return len;
}

void
AbortStream(HQUIC stream) {
	MsQuic->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
}

void
AbortSendStream(HQUIC stream) {
	MsQuic->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND, 0);
}

void FreeStream(HQUIC stream) {
	MsQuic->StreamClose(stream);
}

_IRQL_requires_max_(DISPATCH_LEVEL)
_Function_class_(QUIC_STREAM_CALLBACK)
QUIC_STATUS
QUIC_API
StreamCallback(
	_In_ HQUIC Stream,
	_In_opt_ void* Context,
	_Inout_ QUIC_STREAM_EVENT* Event
)
{
	QUIC_STATUS Status;
	switch (Event->Type) {
		case QUIC_STREAM_EVENT_START_COMPLETE:
			if (Event->START_COMPLETE.Status == QUIC_STATUS_SUCCESS) {
				startStreamCallback(Context, Stream);
			} else {
				// This is a special case.
				// According to the doc, on failure, this is the only Event called.
				// So we proceed with cleaning up everything
				closeStreamCallback(Context, Stream);
				FreeStream(Stream);
			}
			break;
		case QUIC_STREAM_EVENT_SEND_COMPLETE:
			if  (Event->SEND_COMPLETE.ClientContext) {
				freeSendBuffer(((QUIC_BUFFER*)Event->SEND_COMPLETE.ClientContext)->Buffer);
				free(Event->SEND_COMPLETE.ClientContext);
			}
			if (LOGS_ENABLED) {
				printf("[strm][%p] Data sent\n", Stream);
			}
			break;
		case QUIC_STREAM_EVENT_RECEIVE:
			if (LOGS_ENABLED) {
				printf("[strm][%p] Data received, count: %d\n", Stream, Event->RECEIVE.BufferCount);
			}
			if (Event->RECEIVE.BufferCount > 0) {
				uint32_t n = newReadCallback(Context, Stream, Event->RECEIVE.Buffers, Event->RECEIVE.BufferCount);
				if (n > 0) {
					MsQuic->StreamReceiveComplete(Stream, n);
					return QUIC_STATUS_PENDING;
				}
			}
			break;
		case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
			break;
		case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
		case QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE:
			//
			// The peer aborted its send direction of the stream.
			//
			if (LOGS_ENABLED) {
				printf("[strm][%p] Peer aborted\n", Stream);
			}
			abortStreamCallback(Context, Stream);
			break;
		case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
			//
			// Both directions of the stream have been shut down and MsQuic is done
			// with the stream. It can now be safely cleaned up.
			//
			if (LOGS_ENABLED) {
				printf("[strm][%p] Stream done\n", Stream);
			}
			closeStreamCallback(Context, Stream);
			FreeStream(Stream);
			break;
		default:
			break;
	}
	return QUIC_STATUS_SUCCESS;
}

int32_t
AttachAppBuffer(HQUIC Stream, QUIC_BUFFER* buffer) {
	QUIC_STATUS Status;
	if (QUIC_FAILED(Status = MsQuic->StreamProvideReceiveBuffers(Stream, 1, buffer))) {
		printf("Receive buffers failed, 0x%x!\n", Status);
		return -1;
	}
	return 0;
}

void
ShutdownConnection(HQUIC connection) {
	MsQuic->ConnectionShutdown(connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
}

void
AbortConnection(HQUIC connection) {
	MsQuic->ConnectionShutdown(connection, QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT, 0);
}

int32_t
DatagramSendConnection(HQUIC connection, QUIC_BUFFER* buffer) {
	QUIC_STATUS Status;
	if (QUIC_FAILED(Status = MsQuic->DatagramSend(connection, buffer, 1, QUIC_SEND_FLAG_NONE, NULL))) {
		printf("failed to send datagram: %d", Status);
		return -1;
	}
	return 0;
}

HQUIC
CreateStream(
	_In_ HQUIC Connection,
	_In_ int8_t useAppBuffers
)
{
	QUIC_STATUS Status;
	HQUIC Stream = NULL;

	if (LOGS_ENABLED) {
		printf("[strm][%p] Created...\n", Stream);
	}

	enum QUIC_STREAM_OPEN_FLAGS flag = QUIC_STREAM_OPEN_FLAG_NONE;
	if (useAppBuffers == 1) {
		flag |= QUIC_STREAM_OPEN_FLAG_APP_OWNED_BUFFERS;
	}

	if (QUIC_FAILED(Status = MsQuic->StreamOpen(Connection, flag, StreamCallback, Connection, &Stream))) {
		printf("StreamOpen failed, 0x%x!\n", Status);
		return NULL;
	}

	return Stream;
}

int64_t
StartStream(
	_In_ HQUIC Stream,
	_In_ int8_t FailOpen,
	_In_ int8_t useAppBuffers
)
{
	if (LOGS_ENABLED) {
		printf("[strm][%p] Starting...\n", Stream);
	}

	QUIC_STATUS Status;
	enum QUIC_STREAM_START_FLAGS flag = QUIC_STREAM_START_FLAG_NONE;
	if (FailOpen == 1) {
		flag = QUIC_STREAM_START_FLAG_FAIL_BLOCKED;
		flag |= QUIC_STREAM_START_FLAG_IMMEDIATE;
	}

	if (QUIC_FAILED(Status = MsQuic->StreamStart(Stream, flag))) {
		printf("StreamStart failed, 0x%x!\n", Status);
		if (FailOpen == 0) {
			FreeStream(Stream);
		}
		return -1;
	}
	return 0;
}

_IRQL_requires_max_(DISPATCH_LEVEL)
_Function_class_(QUIC_CONNECTION_CALLBACK)
QUIC_STATUS
QUIC_API
ConnectionCallback(
	_In_ HQUIC Connection,
	_In_opt_ void* Context,
	_Inout_ QUIC_CONNECTION_EVENT* Event
)
{
	UNREFERENCED_PARAMETER(Context);
	switch (Event->Type) {
		case QUIC_CONNECTION_EVENT_CONNECTED:
			if (LOGS_ENABLED) {
				printf("[conn][%p] Connected\n", Connection);
			}
			startConnectionCallback(Connection);
			MsQuic->ConnectionSendResumptionTicket(Connection, QUIC_SEND_RESUMPTION_FLAG_NONE, 0, NULL);
			break;
		case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
			if (LOGS_ENABLED) {
				if (Event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status == QUIC_STATUS_CONNECTION_IDLE) {
					printf("[conn][%p] Successfully shut down on idle.\n", Connection);
				} else {
					printf("[conn][%p] Shut down by transport, 0x%x\n", Connection, Event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status);
				}
			}
			closePeerConnectionCallback(Connection);
			break;
		case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
			if (LOGS_ENABLED) {
				printf("[conn][%p] Shut down by peer, 0x%llu\n", Connection, (unsigned long long)Event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode);
			}
			closePeerConnectionCallback(Connection);
			break;
		case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
			if (LOGS_ENABLED) {
				printf("[conn][%p] done\n", Connection);
			}
			closeConnectionCallback(Connection);
			if (!Event->SHUTDOWN_COMPLETE.AppCloseInProgress) {
				MsQuic->ConnectionClose(Connection);
			}
			break;
		case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
			if (LOGS_ENABLED) {
				printf("[strm][%p] Peer started\n", Event->PEER_STREAM_STARTED.Stream);
			}
			newStreamCallback(Connection, Event->PEER_STREAM_STARTED.Stream);
			MsQuic->SetCallbackHandler(Event->PEER_STREAM_STARTED.Stream, (void*)StreamCallback, Connection);
			break;
		case QUIC_CONNECTION_EVENT_RESUMED:
			if (LOGS_ENABLED) {
				printf("[conn][%p] Connection resumed!\n", Connection);
			}
			break;
		case QUIC_CONNECTION_EVENT_PEER_ADDRESS_CHANGED:
			peerAddressChangedCallback(Connection);
			break;
		case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED:
			newDatagramCallback(Connection, Event->DATAGRAM_RECEIVED.Buffer);
			break;
		default:
			break;
	}
	return QUIC_STATUS_SUCCESS;
}

_IRQL_requires_max_(PASSIVE_LEVEL)
_Function_class_(QUIC_LISTENER_CALLBACK)
QUIC_STATUS
QUIC_API
ListenerCallback(
	_In_ HQUIC Listener,
	_In_opt_ void* Context,
	_Inout_ QUIC_LISTENER_EVENT* Event
)
{
	QUIC_STATUS Status = QUIC_STATUS_NOT_SUPPORTED;
	switch (Event->Type) {
		case QUIC_LISTENER_EVENT_NEW_CONNECTION:
			if (LOGS_ENABLED) {
				printf("[conn][%p] new connection\n", Event->NEW_CONNECTION.Connection);
			}
			newConnectionCallback(Listener, Event->NEW_CONNECTION.Connection);
			MsQuic->SetCallbackHandler(Event->NEW_CONNECTION.Connection, (void*)ConnectionCallback, Context);
			Status = MsQuic->ConnectionSetConfiguration(Event->NEW_CONNECTION.Connection, (HQUIC)Context);
			if (QUIC_FAILED(Status)) {
				printf("[conn][%p] new connection failed\n", Event->NEW_CONNECTION.Connection);
				shutConnectionCallback(Event->NEW_CONNECTION.Connection);
			}
			break;
		default:
			break;
	}
	return Status;
}

typedef struct QUIC_CREDENTIAL_CONFIG_HELPER {
	QUIC_CREDENTIAL_CONFIG CredConfig;
	union {
		QUIC_CERTIFICATE_HASH CertHash;
		QUIC_CERTIFICATE_HASH_STORE CertHashStore;
		QUIC_CERTIFICATE_FILE CertFile;
		QUIC_CERTIFICATE_FILE_PROTECTED CertFileProtected;
	};
} QUIC_CREDENTIAL_CONFIG_HELPER;

HQUIC
LoadListenConfiguration(
	_In_ struct QUICConfig cfg
)
{
	QUIC_SETTINGS Settings = {0};
	Settings.ServerResumptionLevel = QUIC_SERVER_RESUME_AND_ZERORTT;
	Settings.IsSet.ServerResumptionLevel = TRUE;
	if (cfg.IdleTimeoutMs != 0) {
		Settings.IdleTimeoutMs = cfg.IdleTimeoutMs;
		Settings.IsSet.IdleTimeoutMs = TRUE;
	}
	if (cfg.KeepAliveMs != 0) {
		Settings.KeepAliveIntervalMs = cfg.KeepAliveMs;
		Settings.IsSet.KeepAliveIntervalMs = TRUE;
	}
	if (cfg.MaxBidiStreams != 0) {
		Settings.PeerBidiStreamCount = cfg.MaxBidiStreams;
		Settings.IsSet.PeerBidiStreamCount = TRUE;
	}
	if (cfg.MaxBindingStatelessOperations != 0) {
		Settings.MaxBindingStatelessOperations = cfg.MaxBindingStatelessOperations;
		Settings.IsSet.MaxBindingStatelessOperations = TRUE;
	}
	if (cfg.MaxStatelessOperations != 0) {
		Settings.MaxStatelessOperations = cfg.MaxStatelessOperations;
		Settings.IsSet.MaxStatelessOperations = TRUE;
	}
	if (cfg.EnableDatagramReceive != 0) {
		Settings.DatagramReceiveEnabled = TRUE;
		Settings.IsSet.DatagramReceiveEnabled = TRUE;
	}
	// Always disable send buffering
	// This lowers extra malloc & copy cost
	Settings.SendBufferingEnabled = FALSE;
	Settings.IsSet.SendBufferingEnabled = TRUE;

	if (cfg.MaxBytesPerKey != 0) {
		Settings.MaxBytesPerKey = cfg.MaxBytesPerKey;
		Settings.IsSet.MaxBytesPerKey = TRUE;
	}

	QUIC_CREDENTIAL_CONFIG_HELPER config = {0};
	config.CredConfig.Flags = QUIC_CREDENTIAL_FLAG_NONE;

	config.CertFile.CertificateFile = cfg.certFile;
	config.CertFile.PrivateKeyFile = cfg.keyFile;
	config.CredConfig.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
	config.CredConfig.CertificateFile = &config.CertFile;
	QUIC_STATUS Status = QUIC_STATUS_SUCCESS;
	HQUIC configuration = NULL;
	if (QUIC_FAILED(Status = MsQuic->ConfigurationOpen(Registration, &cfg.Alpn, 1, &Settings,
													sizeof(Settings), NULL, &configuration))) {
		printf("ConfigurationOpen failed, 0x%x!\n", Status);
		return NULL;
	}
	if (QUIC_FAILED(Status = MsQuic->ConfigurationLoadCredential(configuration, &config.CredConfig))) {
		printf("ConfigurationLoadCredential failed, 0x%x!\n", Status);
		return NULL;
	}
	return configuration;
}

HQUIC
CreateListener(
	_In_ HQUIC configuration
)
{
	QUIC_STATUS Status;
	HQUIC listener = NULL;

	if (QUIC_FAILED(Status = MsQuic->ListenerOpen(Registration, ListenerCallback, configuration, &listener))) {
		printf("ListenerOpen failed, 0x%x!\n", Status);
		return listener;
	}

	return listener;
}

int
StartListener(
	_In_ HQUIC listener,
	_In_ const char* addr,
	_In_ uint16_t port,
	_In_ QUIC_BUFFER Alpn
)
{
	QUIC_STATUS Status;

	QUIC_ADDR quicAddr = {0};
	customQuicAddrFromString(addr, port, &quicAddr);
	customQuicAddrSetFamily(&quicAddr, QUIC_ADDRESS_FAMILY_UNSPEC);
	customQuicAddrSetPort(&quicAddr, port);

	if (QUIC_FAILED(Status = MsQuic->ListenerStart(listener, &Alpn, 1, &quicAddr))) {
		printf("ListenerStart failed, 0x%x!\n", Status);
		return -1;
	}

	if (LOGS_ENABLED) {
		printf("Listen to %s:%d\n", addr, port);
	}

	return 0;
}

void CloseListener(HQUIC listener, HQUIC configuration) {
	MsQuic->ListenerClose(listener);
	MsQuic->ConfigurationClose(configuration);
}

HQUIC
LoadDialConfiguration(struct QUICConfig cfg)
{
	HQUIC configuration;
	QUIC_SETTINGS Settings = {0};
	Settings.IdleTimeoutMs = cfg.IdleTimeoutMs;
	Settings.IsSet.IdleTimeoutMs = TRUE;
	Settings.PeerBidiStreamCount = cfg.MaxBidiStreams;
	Settings.IsSet.PeerBidiStreamCount = TRUE;
	if (cfg.KeepAliveMs != 0)  {
		Settings.KeepAliveIntervalMs = cfg.KeepAliveMs;
		Settings.IsSet.KeepAliveIntervalMs = TRUE;
	}

	if (cfg.EnableDatagramReceive != 0) {
		Settings.DatagramReceiveEnabled = TRUE;
		Settings.IsSet.DatagramReceiveEnabled = TRUE;
	}
	if (cfg.DisableSendBuffering != 0) {
		Settings.SendBufferingEnabled = FALSE;
		Settings.IsSet.SendBufferingEnabled = TRUE;
	}
	if (cfg.MaxBytesPerKey != 0) {
		Settings.MaxBytesPerKey = cfg.MaxBytesPerKey;
		Settings.IsSet.MaxBytesPerKey = TRUE;
	}

	QUIC_CREDENTIAL_CONFIG CredConfig = {0};
	CredConfig.Type = QUIC_CREDENTIAL_TYPE_NONE;
	CredConfig.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
	if (cfg.DisableCertificateValidation == TRUE) {
		CredConfig.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
	}

	QUIC_STATUS Status = QUIC_STATUS_SUCCESS;
	if (QUIC_FAILED(Status = MsQuic->ConfigurationOpen(Registration, &cfg.Alpn, 1,
													&Settings, sizeof(Settings), NULL, &configuration))) {
		printf("ConfigurationOpen failed, 0x%x!\n", Status);
		return NULL;
	}

	if (QUIC_FAILED(Status = MsQuic->ConfigurationLoadCredential(configuration, &CredConfig))) {
		printf("ConfigurationLoadCredential failed, 0x%x!\n", Status);
		return NULL;
	}

	return configuration;
}

HQUIC
OpenConnection()
{
	QUIC_STATUS Status;
	HQUIC connection = NULL;

	if (QUIC_FAILED(Status = MsQuic->ConnectionOpen(Registration, ConnectionCallback, NULL, &connection))) {
		printf("ConnectionOpen failed, 0x%x!\n", Status);
		return NULL;
	}

	return connection;
}

void
StartConnection(
	_In_ HQUIC connection ,
	_In_ const char* addr,
	_In_ uint16_t port,
	_In_ struct QUICConfig cfg
)
{
	QUIC_STATUS Status;

	HQUIC configuration = LoadDialConfiguration(cfg);
	if (!configuration) {
		printf("Connection Load dial error!\n");
		return;
	}

	if (QUIC_FAILED(Status = MsQuic->ConnectionStart(connection, configuration, QUIC_ADDRESS_FAMILY_UNSPEC,
												  addr, port))) {
		printf("ConnectionStart failed, 0x%x!\n", Status);
		MsQuic->ConnectionClose(connection);
		return;
	}
}

static const QUIC_REGISTRATION_CONFIG RegConfig = { "go-msquic", QUIC_EXECUTION_PROFILE_LOW_LATENCY };

// This setup is tight to the process lifetime
int
MsQuicSetup()
{
	QUIC_STATUS Status = QUIC_STATUS_SUCCESS;
	if (QUIC_FAILED(Status = MsQuicOpen2(&MsQuic))) {
		printf("MsQuicOpen2 failed, 0x%x!\n", Status);
		goto Error;
	}

	if (QUIC_FAILED(Status = MsQuic->RegistrationOpen(&RegConfig, &Registration))) {
		printf("RegistrationOpen failed, 0x%x!\n", Status);
		goto Error;
	}

	return 0;

Error:
	if (MsQuic != NULL) {
		if (Registration != NULL) {
			MsQuic->RegistrationClose(Registration);
		}
		MsQuicClose(MsQuic);
	}
	return (int)Status;
}

int
GetRemoteAddr(
	_In_ HQUIC conn,
	_Out_ QUIC_ADDR* addr,
	_Out_ uint32_t* addrLen
)
{
	if (MsQuic->GetParam(conn, QUIC_PARAM_CONN_REMOTE_ADDRESS, addrLen, addr) != QUIC_STATUS_SUCCESS) {
		return -1; // Failed to retrieve
	}
	return 0;
}

extern uint32_t CxPlatProcessorCount;
int GetPerfCounters(uint64_t *Counters) {
	uint32_t BufferLength = sizeof(uint64_t)*QUIC_PERF_COUNTER_MAX;
	MsQuic->GetParam(
		NULL,
		QUIC_PARAM_GLOBAL_PERF_COUNTERS,
		&BufferLength,
		Counters);
	return CxPlatProcessorCount;
}
