#include "./inc/msquic.h"
#include <stdlib.h>

#define UNREFERENCED_PARAMETER(P) (void)(P)

#ifndef UNREFERENCED_PARAMETER
#define UNREFERENCED_PARAMETER(P) (void)(P)
#endif

#define LOGS_ENABLED 1

// Go bindings
extern void newConnectionCallback(HQUIC, HQUIC);
extern void newStreamCallback(HQUIC, HQUIC);
extern void newReadCallback(HQUIC, uint8_t *data, int64_t len);

static HQUIC Registration;
static const QUIC_BUFFER Alpn = { sizeof("quic-backconnect") - 1, (uint8_t*)"quic-backconnect" };
static const QUIC_API_TABLE* MsQuic;
static const uint64_t IdleTimeoutMs = 5000;

struct ConnectionContext {
	HQUIC configuration;
};

struct QUICConfig {
	int DisableCertificateValidation;
	int MaxBidiStreams;
	int IdleTimeoutMs;
	int KeepAliveMs;
    char * keyFile;
    char * certFile;
};

static
int64_t
StreamWrite(
    _In_ HQUIC Stream,
	_In_ uint8_t *array,
	_Inout_ int64_t *len
    )
{
    char* SendBufferRaw = malloc(sizeof(QUIC_BUFFER) + *len);
    if (SendBufferRaw == NULL) {
        printf("SendBuffer allocation failed!\n");
        MsQuic->StreamShutdown(Stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
		*len = 0;
        return -1;
    }
	memcpy(SendBufferRaw+sizeof(QUIC_BUFFER), array, *len);
    QUIC_BUFFER* SendBuffer = (QUIC_BUFFER*)SendBufferRaw;
    SendBuffer->Buffer = (uint8_t*)SendBufferRaw + sizeof(QUIC_BUFFER);
    SendBuffer->Length = *len;

    //printf("[strm][%p] Sending data...\n", Stream);

    QUIC_STATUS Status;
    if (QUIC_FAILED(Status = MsQuic->StreamSend(Stream, SendBuffer, 1, QUIC_SEND_FLAG_NONE, SendBuffer))) {
        printf("StreamSend failed, 0x%x!\n", Status);
        free(SendBufferRaw);
        MsQuic->StreamShutdown(Stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
		return -1;
    }
	//free(SendBufferRaw);
	return 0;
}

static
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
    UNREFERENCED_PARAMETER(Context);
    switch (Event->Type) {
    case QUIC_STREAM_EVENT_SEND_COMPLETE:
        //
        // A previous StreamSend call has completed, and the context is being
        // returned back to the app.
        //
        free(Event->SEND_COMPLETE.ClientContext);
		if (LOGS_ENABLED) {
			printf("[strm][%p] Data sent\n", Stream);
		}
        break;
    case QUIC_STREAM_EVENT_RECEIVE:
		if (LOGS_ENABLED) {
			printf("[strm][%p] Data received, count: %d\n", Stream, Event->RECEIVE.BufferCount);
		}
		for (uint32_t i = 0; i < Event->RECEIVE.BufferCount; i++) {
			newReadCallback(Stream, Event->RECEIVE.Buffers[i].Buffer, Event->RECEIVE.Buffers[i].Length);
		}
		MsQuic->StreamReceiveComplete(Stream, Event->RECEIVE.TotalBufferLength);
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
		if (LOGS_ENABLED) {
			printf("[strm][%p] Peer shut down\n", Stream);
		}
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        //
        // The peer aborted its send direction of the stream.
        //
		if (LOGS_ENABLED) {
			printf("[strm][%p] Peer aborted\n", Stream);
		}
        MsQuic->StreamShutdown(Stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
        break;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        //
        // Both directions of the stream have been shut down and MsQuic is done
        // with the stream. It can now be safely cleaned up.
        //
		if (LOGS_ENABLED) {
			printf("[strm][%p] Stream done\n", Stream);
		}
        MsQuic->StreamClose(Stream);
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

static
HQUIC
OpenStream(
    _In_ HQUIC Connection
    )
{
    QUIC_STATUS Status;
    HQUIC Stream = NULL;

    //
    // Create/allocate a new bidirectional stream. The stream is just allocated
    // and no QUIC stream identifier is assigned until it's started.
    //
    if (QUIC_FAILED(Status = MsQuic->StreamOpen(Connection, QUIC_STREAM_OPEN_FLAG_NONE, StreamCallback, NULL, &Stream))) {
        printf("StreamOpen failed, 0x%x!\n", Status);
		return NULL;
    }

	if (LOGS_ENABLED) {
		printf("[strm][%p] Starting...\n", Stream);
	}

    //
    // Starts the bidirectional stream. By default, the peer is not notified of
    // the stream being started until data is sent on the stream.
    //
    if (QUIC_FAILED(Status = MsQuic->StreamStart(Stream, QUIC_STREAM_START_FLAG_NONE))) {
        printf("StreamStart failed, 0x%x!\n", Status);
        MsQuic->StreamClose(Stream);
		return NULL;
    }
	return Stream;
}

static
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
        printf("[conn][%p] Connected\n", Connection);
        MsQuic->ConnectionSendResumptionTicket(Connection, QUIC_SEND_RESUMPTION_FLAG_NONE, 0, NULL);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        if (Event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status == QUIC_STATUS_CONNECTION_IDLE) {
            printf("[conn][%p] Successfully shut down on idle.\n", Connection);
        } else {
            printf("[conn][%p] Shut down by transport, 0x%x\n", Connection, Event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status);
        }
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        printf("[conn][%p] Shut down by peer, 0x%llu\n", Connection, (unsigned long long)Event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        printf("[conn][%p] done\n", Connection);
        MsQuic->ConnectionClose(Connection);
        break;
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
        printf("[strm][%p] Peer started\n", Event->PEER_STREAM_STARTED.Stream);
        MsQuic->SetCallbackHandler(Event->PEER_STREAM_STARTED.Stream, (void*)StreamCallback, NULL);
		newStreamCallback(Connection, Event->PEER_STREAM_STARTED.Stream);
        break;
    case QUIC_CONNECTION_EVENT_RESUMED:
        printf("[conn][%p] Connection resumed!\n", Connection);
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

static
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
    UNREFERENCED_PARAMETER(Listener);
    UNREFERENCED_PARAMETER(Context);
	HQUIC config = NULL;
	if (Context != NULL) {
		config = ((struct ConnectionContext*) Context) -> configuration;
	}
    QUIC_STATUS Status = QUIC_STATUS_NOT_SUPPORTED;
    switch (Event->Type) {
    case QUIC_LISTENER_EVENT_NEW_CONNECTION:
        //
        // A new connection is being attempted by a client. For the handshake to
        // proceed, the server must provide a configuration for QUIC to use. The
        // app MUST set the callback handler before returning.
        //
        MsQuic->SetCallbackHandler(Event->NEW_CONNECTION.Connection, (void*)ConnectionCallback, Context);
        Status = MsQuic->ConnectionSetConfiguration(Event->NEW_CONNECTION.Connection, config);
		newConnectionCallback(Listener, Event->NEW_CONNECTION.Connection);
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

static
HQUIC
LoadListenConfiguration(struct QUICConfig cfg)
{
    QUIC_SETTINGS Settings = {0};
    Settings.IdleTimeoutMs = cfg.IdleTimeoutMs;
    Settings.IsSet.IdleTimeoutMs = TRUE;
    Settings.ServerResumptionLevel = QUIC_SERVER_RESUME_AND_ZERORTT;
    Settings.IsSet.ServerResumptionLevel = TRUE;
    Settings.PeerBidiStreamCount = cfg.MaxBidiStreams;
    Settings.IsSet.PeerBidiStreamCount = TRUE;

    QUIC_CREDENTIAL_CONFIG_HELPER config;
    memset(&config, 0, sizeof(config));
    config.CredConfig.Flags = QUIC_CREDENTIAL_FLAG_NONE;

	config.CertFile.CertificateFile = cfg.certFile;
	config.CertFile.PrivateKeyFile = cfg.keyFile;
	config.CredConfig.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
	config.CredConfig.CertificateFile = &config.CertFile;
    QUIC_STATUS Status = QUIC_STATUS_SUCCESS;
	HQUIC configuration;
    if (QUIC_FAILED(Status = MsQuic->ConfigurationOpen(Registration, &Alpn, 1, &Settings,
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

static
HQUIC
Listen(
	_In_ const char* addr,
	_In_ uint16_t port,
	_In_ struct QUICConfig cfg
)
{
    QUIC_STATUS Status;
    HQUIC listener = NULL;

    QUIC_ADDR quicAddr = {0};
	QuicAddrFromString(addr, port, &quicAddr);
    QuicAddrSetFamily(&quicAddr, QUIC_ADDRESS_FAMILY_UNSPEC);
    QuicAddrSetPort(&quicAddr, port);

    HQUIC configuration = LoadListenConfiguration(cfg);
	if (!configuration) {
        return listener;
    }

	struct ConnectionContext * ctx = malloc(sizeof(struct ConnectionContext));
	ctx->configuration = configuration;

    if (QUIC_FAILED(Status = MsQuic->ListenerOpen(Registration, ListenerCallback, ctx, &listener))) {
        printf("ListenerOpen failed, 0x%x!\n", Status);
		return listener;
    }

    if (QUIC_FAILED(Status = MsQuic->ListenerStart(listener, &Alpn, 1, &quicAddr))) {
        printf("ListenerStart failed, 0x%x!\n", Status);
		return listener;
    }

	if (LOGS_ENABLED) {
		printf("Listen to %s:%d\n", addr, port);
	}

	return listener;
}

static
void CloseListener(HQUIC listener) {
	MsQuic->ListenerClose(listener);
}

static
void CloseConfiguration(HQUIC configuration) {
	if (configuration != NULL) {
		MsQuic->ConfigurationClose(configuration);
	}
}

static
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

    QUIC_CREDENTIAL_CONFIG CredConfig;
    memset(&CredConfig, 0, sizeof(CredConfig));
    CredConfig.Type = QUIC_CREDENTIAL_TYPE_NONE;
    CredConfig.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
	if (cfg.DisableCertificateValidation == TRUE) {
        CredConfig.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
	}

    QUIC_STATUS Status = QUIC_STATUS_SUCCESS;
    if (QUIC_FAILED(Status = MsQuic->ConfigurationOpen(Registration, &Alpn, 1,
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

static
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
		printf("[conn][%p] Connect... %s\n", connection, addr);
	}

    if (QUIC_FAILED(Status = MsQuic->ConnectionStart(connection, configuration, QUIC_ADDRESS_FAMILY_UNSPEC,
		addr, port))) {
        printf("ConnectionStart failed, 0x%x!\n", Status);
        MsQuic->ConnectionClose(connection);
		return NULL;
    }

	return connection;

}

static const QUIC_REGISTRATION_CONFIG RegConfig = { "quic-backconnect", QUIC_EXECUTION_PROFILE_LOW_LATENCY };

// This setup is tight to the process lifetime
static
int
MsQuicSetup() {
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

static
int
GetRemoteAddr(
HQUIC conn, struct sockaddr_storage* addr, uint32_t* addrLen){
 if (MsQuic->GetParam(conn, QUIC_PARAM_CONN_REMOTE_ADDRESS, addrLen, addr) != QUIC_STATUS_SUCCESS) {
        return -1; // Failed to retrieve
    }

    return 0;
}
