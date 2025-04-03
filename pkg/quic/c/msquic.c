#include "inc/msquic.h"
#include <stdlib.h>

#define UNREFERENCED_PARAMETER(P) (void)(P)

#ifndef UNREFERENCED_PARAMETER
#define UNREFERENCED_PARAMETER(P) (void)(P)
#endif

#define LOGS_ENABLED 0

#include "utils.c"

// Go bindings
extern void newConnectionCallback(HQUIC, HQUIC);
extern void newStreamCallback(HQUIC, HQUIC);
extern void newReadCallback(HQUIC, HQUIC, uint8_t *data, int64_t len);
extern void completeWriteCallback(HQUIC, HQUIC);
extern void closeConnectionCallback(HQUIC);
extern void closeStreamCallback(HQUIC,HQUIC);

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
};

int64_t
StreamWrite(
    _In_ HQUIC Stream,
	_In_ uint8_t *array,
	_In_ int64_t len
    )
{
    char* SendBufferRaw = malloc(sizeof(QUIC_BUFFER) + len);
    if (SendBufferRaw == NULL) {
        printf("SendBuffer allocation failed!\n");
        MsQuic->StreamShutdown(Stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
        return -1;
    }
	memcpy(SendBufferRaw+sizeof(QUIC_BUFFER), array, len);
    QUIC_BUFFER* SendBuffer = (QUIC_BUFFER*)SendBufferRaw;
    SendBuffer->Buffer = (uint8_t*)SendBufferRaw + sizeof(QUIC_BUFFER);
    SendBuffer->Length = len;

    QUIC_STATUS Status;
    if (QUIC_FAILED(Status = MsQuic->StreamSend(Stream, SendBuffer, 1, QUIC_SEND_FLAG_NONE, SendBuffer))) {
        printf("StreamSend failed, 0x%x!\n", Status);
        free(SendBufferRaw);
        MsQuic->StreamShutdown(Stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
		return -1;
    }
	return len;
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
    switch (Event->Type) {
    case QUIC_STREAM_EVENT_SEND_COMPLETE:
		completeWriteCallback(Context, Stream);
		if  (Event->SEND_COMPLETE.ClientContext) {
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
		for (uint32_t i = 0; i < Event->RECEIVE.BufferCount; i++) {
			newReadCallback(Context, Stream, Event->RECEIVE.Buffers[i].Buffer, Event->RECEIVE.Buffers[i].Length);
		}
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
		if (LOGS_ENABLED) {
			printf("[strm][%p] Peer shut down\n", Stream);
		}
        MsQuic->StreamShutdown(Stream, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0);
        break;
	case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
    case QUIC_STREAM_EVENT_SEND_SHUTDOWN_COMPLETE:
        //
        // The peer aborted its send direction of the stream.
        //
		if (LOGS_ENABLED) {
			printf("[strm][%p] Peer aborted\n", Stream);
		}
        MsQuic->StreamShutdown(Stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT|QUIC_STREAM_SHUTDOWN_FLAG_IMMEDIATE, 0);
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
		if (!Event->SHUTDOWN_COMPLETE.AppCloseInProgress) {
			MsQuic->StreamClose(Stream);
		}
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

void
ShutdownConnection(HQUIC connection) {
	MsQuic->ConnectionShutdown(connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
}

void
AbortConnection(HQUIC connection) {
	MsQuic->ConnectionShutdown(connection, QUIC_CONNECTION_SHUTDOWN_FLAG_SILENT, 0);
}

void
ShutdownStream(HQUIC stream) {
	// This only shutdown sending part
	MsQuic->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL|QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 0);
}

void
AbortStream(HQUIC stream) {
	MsQuic->StreamShutdown(stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
}

HQUIC
CreateStream(
    _In_ HQUIC Connection
    )
{
    QUIC_STATUS Status;
    HQUIC Stream = NULL;

	if (LOGS_ENABLED) {
		printf("[strm][%p] Created...\n", Stream);
	}

    if (QUIC_FAILED(Status = MsQuic->StreamOpen(Connection, QUIC_STREAM_OPEN_FLAG_NONE, StreamCallback, Connection, &Stream))) {
        printf("StreamOpen failed, 0x%x!\n", Status);
		return NULL;
    }

	return Stream;
}

uint64_t
StartStream(
    _In_ HQUIC Stream,
    _In_ int8_t FailOpen
    )
{
	if (LOGS_ENABLED) {
		printf("[strm][%p] Starting...\n", Stream);
	}

	enum QUIC_STREAM_START_FLAGS flag = QUIC_STREAM_START_FLAG_NONE;
	if (FailOpen == 1) {
		flag = QUIC_STREAM_START_FLAG_FAIL_BLOCKED;
	}

    QUIC_STATUS Status;
    if (QUIC_FAILED(Status = MsQuic->StreamStart(Stream, flag))) {
        printf("StreamStart failed, 0x%x!\n", Status);
        MsQuic->StreamClose(Stream);
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
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
		if (LOGS_ENABLED) {
			printf("[conn][%p] Shut down by peer, 0x%llu\n", Connection, (unsigned long long)Event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode);
		}
        MsQuic->ConnectionShutdown(Connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
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
        Status = MsQuic->ConnectionSetConfiguration(Event->NEW_CONNECTION.Connection, (HQUIC)Context);
		if (LOGS_ENABLED) {
			printf("[conn][%p] new connection\n", Event->NEW_CONNECTION.Connection);
		}
		if (QUIC_SUCCEEDED(Status)) {
			newConnectionCallback(Listener, Event->NEW_CONNECTION.Connection);
			MsQuic->SetCallbackHandler(Event->NEW_CONNECTION.Connection, (void*)ConnectionCallback, Context);
		} else {
			MsQuic->ConnectionShutdown(Event->NEW_CONNECTION.Connection, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
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
Listen(
	_In_ const char* addr,
	_In_ uint16_t port,
	_In_ HQUIC configuration,
	_In_ QUIC_BUFFER Alpn
)
{
    QUIC_STATUS Status;
    HQUIC listener = NULL;

    if (QUIC_FAILED(Status = MsQuic->ListenerOpen(Registration, ListenerCallback, configuration, &listener))) {
        printf("ListenerOpen failed, 0x%x!\n", Status);
		return listener;
    }

    QUIC_ADDR quicAddr = {0};
	customQuicAddrFromString(addr, port, &quicAddr);
    customQuicAddrSetFamily(&quicAddr, QUIC_ADDRESS_FAMILY_UNSPEC);
    customQuicAddrSetPort(&quicAddr, port);

    if (QUIC_FAILED(Status = MsQuic->ListenerStart(listener, &Alpn, 1, &quicAddr))) {
        printf("ListenerStart failed, 0x%x!\n", Status);
		return listener;
    }

	if (LOGS_ENABLED) {
		printf("Listen to %s:%d\n", addr, port);
	}

	return listener;
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
	Settings.KeepAliveIntervalMs = cfg.KeepAliveMs;
	Settings.IsSet.KeepAliveIntervalMs = TRUE;

	if (cfg.EnableDatagramReceive != 0) {
		Settings.DatagramReceiveEnabled = TRUE;
		Settings.IsSet.DatagramReceiveEnabled = TRUE;
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
DialConnection(
	_In_ const char* addr,
	_In_ uint16_t port,
	_In_ struct QUICConfig cfg
)
{
    QUIC_STATUS Status;

    HQUIC configuration = LoadDialConfiguration(cfg);
	if (!configuration) {
        return NULL;
    }

    HQUIC connection = NULL;

    if (QUIC_FAILED(Status = MsQuic->ConnectionOpen(Registration, ConnectionCallback, NULL, &connection))) {
        printf("ConnectionOpen failed, 0x%x!\n", Status);
		return NULL;
    }

	if (LOGS_ENABLED) {
		printf("[conn][%p] Connect to %s\n", connection, addr);
	}

    if (QUIC_FAILED(Status = MsQuic->ConnectionStart(connection, configuration, QUIC_ADDRESS_FAMILY_UNSPEC,
		addr, port))) {
        printf("ConnectionStart failed, 0x%x!\n", Status);
        MsQuic->ConnectionClose(connection);
		return NULL;
    }

	return connection;

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
	_Out_ struct sockaddr_storage* addr,
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
