static
void
customQuicAddrSetFamily(
    _In_ QUIC_ADDR* Addr,
    _In_ QUIC_ADDRESS_FAMILY Family
    )
{
    Addr->Ip.sa_family = Family;
}

static
void
customQuicAddrSetPort(
    _Out_ QUIC_ADDR* Addr,
    _In_ uint16_t Port
    )
{
    if (QUIC_ADDRESS_FAMILY_INET == Addr->Ip.sa_family) {
        Addr->Ipv4.sin_port = htons(Port);
    } else {
        Addr->Ipv6.sin6_port = htons(Port);
    }
}

static
BOOLEAN
customQuicAddr4FromString(
    _In_z_ const char* AddrStr,
    _Out_ QUIC_ADDR* Addr
    )
{
    if (AddrStr[0] == '[') {
        return FALSE;
    }

    const char* PortStart = strchr(AddrStr, ':');
    if (PortStart != NULL) {
        if (strchr(PortStart+1, ':') != NULL) {
            return FALSE;
        }

        char TmpAddrStr[16];
        size_t AddrLength = PortStart - AddrStr;
        if (AddrLength >= sizeof(TmpAddrStr)) {
            return FALSE;
        }
        memcpy(TmpAddrStr, AddrStr, AddrLength);
        TmpAddrStr[AddrLength] = '\0';

        if (inet_pton(AF_INET, TmpAddrStr, &Addr->Ipv4.sin_addr) != 1) {
            return FALSE;
        }
        Addr->Ipv4.sin_port = htons(atoi(PortStart+1));
    } else {
        if (inet_pton(AF_INET, AddrStr, &Addr->Ipv4.sin_addr) != 1) {
            return FALSE;
        }
    }
    Addr->Ip.sa_family = QUIC_ADDRESS_FAMILY_INET;
    return TRUE;
}

static
BOOLEAN
customQuicAddr6FromString(
    _In_z_ const char* AddrStr,
    _Out_ QUIC_ADDR* Addr
    )
{
    if (AddrStr[0] == '[') {
        const char* BracketEnd = strchr(AddrStr, ']');
        if (BracketEnd == NULL || *(BracketEnd+1) != ':') {
            return FALSE;
        }

        char TmpAddrStr[64];
        size_t AddrLength = BracketEnd - AddrStr - 1;
        if (AddrLength >= sizeof(TmpAddrStr)) {
            return FALSE;
        }
        memcpy(TmpAddrStr, AddrStr + 1, AddrLength);
        TmpAddrStr[AddrLength] = '\0';

        if (inet_pton(AF_INET6, TmpAddrStr, &Addr->Ipv6.sin6_addr) != 1) {
            return FALSE;
        }
        Addr->Ipv6.sin6_port = htons(atoi(BracketEnd+2));
    } else {
        if (inet_pton(AF_INET6, AddrStr, &Addr->Ipv6.sin6_addr) != 1) {
            return FALSE;
        }
    }
    Addr->Ip.sa_family = QUIC_ADDRESS_FAMILY_INET6;
    return TRUE;
}

static
BOOLEAN
customQuicAddrFromString(
    _In_z_ const char* AddrStr,
    _In_ uint16_t Port, // Host byte order
    _Out_ QUIC_ADDR* Addr
    )
{
    Addr->Ipv4.sin_port = htons(Port);
    return
        customQuicAddr4FromString(AddrStr, Addr) ||
        customQuicAddr6FromString(AddrStr, Addr);
}
