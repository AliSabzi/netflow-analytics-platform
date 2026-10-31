package com.novindata.netflow.core.validation;

import com.novindata.netflow.core.avro.NetFlowV2Record;
import com.novindata.netflow.core.avro.NetFlowV2OutputRecord;

import java.util.regex.Pattern;

public final class NetFlowValidator {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
            "^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}" +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    );

    private static final int PORT_MIN = 0;
    private static final int PORT_MAX = 65535;
    private static final int BYTE_FIELD_MIN = 0;
    private static final int BYTE_FIELD_MAX = 255;

    private NetFlowValidator() {
    }

    public static boolean validate(NetFlowV2OutputRecord record) {
        requireRecord(record);

        validateAddresses(record.getIPV4SRCADDR(), record.getIPV4DSTADDR());
        validatePorts(record.getL4SRCPORT(), record.getL4DSTPORT());
        validateProtocolFields(record.getPROTOCOL(), record.getL7PROTO());
        validateTcpFlags(
                record.getTCPFLAGS(),
                record.getCLIENTTCPFLAGS(),
                record.getSERVERTCPFLAGS());
        validateTcpWindows(record.getTCPWINMAXIN(), record.getTCPWINMAXOUT());
        validateIcmpFields(record.getICMPTYPE(), record.getICMPIPV4TYPE());
        validateDnsFields(
                record.getDNSQUERYID(),
                record.getDNSQUERYTYPE(),
                record.getDNSTTLANSWER());
        validateFtpField(record.getFTPCOMMANDRETCODE());

        return true;
    }

    public static boolean validate(NetFlowV2Record record) {
        requireRecord(record);

        validateAddresses(record.getIPV4SRCADDR(), record.getIPV4DSTADDR());
        validatePorts(record.getL4SRCPORT(), record.getL4DSTPORT());
        validateProtocolFields(record.getPROTOCOL(), record.getL7PROTO());
        validateTcpFlags(record.getTCPFLAGS(),record.getCLIENTTCPFLAGS(),record.getSERVERTCPFLAGS());
        validateTcpWindows(record.getTCPWINMAXIN(), record.getTCPWINMAXOUT());
        validateIcmpFields(record.getICMPTYPE(), record.getICMPIPV4TYPE());
        validateDnsFields(record.getDNSQUERYID(),record.getDNSQUERYTYPE(),record.getDNSTTLANSWER());
        validateFtpField(record.getFTPCOMMANDRETCODE());

        return true;
    }


    private static void validateAddresses(
            CharSequence sourceAddress,
            CharSequence destinationAddress) {
        String src = String.valueOf(sourceAddress);
        String dst = String.valueOf(destinationAddress);

        if (!isValidIPv4(src)) {
            throw new NetFlowValidationException("IPV4_SRC_ADDR is not a valid IPv4 address: " + src);
        }
        if (!isValidIPv4(dst)) {
            throw new NetFlowValidationException("IPV4_DST_ADDR is not a valid IPv4 address: " + dst);
        }
    }

    private static boolean isValidIPv4(String ip) {
        return ip != null && IPV4_PATTERN.matcher(ip).matches();
    }


    private static void validatePorts(int sourcePort, int destinationPort) {
        if (!isInRange(sourcePort, PORT_MIN, PORT_MAX)) {
            throw new NetFlowValidationException("L4_SRC_PORT out of range [" + PORT_MIN + ", " + PORT_MAX + "]: " + sourcePort);
        }
        if (!isInRange(destinationPort, PORT_MIN, PORT_MAX)) {
            throw new NetFlowValidationException("L4_DST_PORT out of range [" + PORT_MIN + ", " + PORT_MAX + "]: " + destinationPort);
        }
    }

    private static void validateProtocolFields(int protocol, Double l7Proto) {
        if (!isInRange(protocol, 0, 255)) {
            throw new NetFlowValidationException("PROTOCOL out of range [0, 255]: " + protocol);
        }
        if (l7Proto == null || l7Proto < 0.0) {
            throw new NetFlowValidationException("L7_PROTO must be non-negative: " + l7Proto);
        }
    }


    private static void validateTcpFlags(
            int tcpFlags,
            int clientTcpFlags,
            int serverTcpFlags) {
        if (!isInRange(tcpFlags, BYTE_FIELD_MIN, BYTE_FIELD_MAX)) {
            throw new NetFlowValidationException("TCP_FLAGS out of range [0, 255]: " + tcpFlags);
        }
        if (!isInRange(clientTcpFlags, BYTE_FIELD_MIN, BYTE_FIELD_MAX)) {
            throw new NetFlowValidationException("CLIENT_TCP_FLAGS out of range [0, 255]: " + clientTcpFlags);
        }
        if (!isInRange(serverTcpFlags, BYTE_FIELD_MIN, BYTE_FIELD_MAX)) {
            throw new NetFlowValidationException("SERVER_TCP_FLAGS out of range [0, 255]: " + serverTcpFlags);
        }
    }


    private static void validateTcpWindows(int maxIn, int maxOut) {
        if (!isInRange(maxIn, PORT_MIN, PORT_MAX)) {
            throw new NetFlowValidationException("TCP_WIN_MAX_IN out of range [" + PORT_MIN + ", " + PORT_MAX + "]: " + maxIn);
        }
        if (!isInRange(maxOut, PORT_MIN, PORT_MAX)) {
            throw new NetFlowValidationException("TCP_WIN_MAX_OUT out of range [" + PORT_MIN + ", " + PORT_MAX + "]: " + maxOut);
        }
    }


    private static void validateIcmpFields(int icmpType, int icmpIpv4Type) {
        if (!isInRange(icmpType, 0, 65535)) {
            throw new NetFlowValidationException("ICMP_TYPE out of range [0, 65535]: " + icmpType);
        }
        if (!isInRange(icmpIpv4Type, 0, 255)) {
            throw new NetFlowValidationException("ICMP_IPV4_TYPE out of range [0, 255]: " + icmpIpv4Type);
        }
    }


    private static void validateDnsFields(int queryId, int queryType, long ttlAnswer) {
        if (!isInRange(queryId, 0, 65535)) {
            throw new NetFlowValidationException("DNS_QUERY_ID out of range [0, 65535]: " + queryId);
        }
        if (queryType < 0) {
            throw new NetFlowValidationException("DNS_QUERY_TYPE must be non-negative: " + queryType);
        }
        requireNonNegative(ttlAnswer, "DNS_TTL_ANSWER");
    }


    private static void validateFtpField(int code) {
        boolean valid = code == -1 || code == 0 || isInRange(code, 100, 599);
        if (!valid) {
            throw new NetFlowValidationException("FTP_COMMAND_RET_CODE is not -1, 0, or in range [100, 599]: " + code);
        }
    }


    private static void requireRecord(Object record) {
        if (record == null) {
            throw new NetFlowValidationException("Record must not be null");
        }
    }

    private static boolean isInRange(int value, int min, int max) {
        return value >= min && value <= max;
    }

    private static void requireNonNegative(long value, String fieldName) {
        if (value < 0) {
            throw new NetFlowValidationException(fieldName + " must be non-negative: " + value);
        }
    }
}
