#!/bin/sh
# sysmon.sh - once-a-second host sampler for a bench/social run. Reads /proc
# only and writes CSV to stdout; needs nothing beyond a stock Debian (sh, awk,
# date, sleep).
#
#   sysmon.sh -p PID -d DISK -i IFACE [-n SAMPLES] > sysmon.csv
#
#   -p PID      process whose resident set size (VmRSS) is recorded
#   -d DISK     block device as named in /proc/diskstats (nvme0n1, sda, ...)
#   -i IFACE    network interface as named in /proc/net/dev (eth0, lo, ...)
#   -n SAMPLES  stop after this many samples (default: run until killed)
#
# Any of -p/-d/-i may be omitted; its columns are then left empty. The first
# line of output is a header. Rates are computed over the real time between
# two samples, not an assumed one second.
#
# Columns: epoch_s, cpu_pct (all cores), cpu0_pct..cpuN_pct, mem_available_mb,
# cached_mb, swap_used_mb, rss_mb, disk_read_mbps, disk_write_mbps,
# disk_read_iops, disk_write_iops, disk_busy_pct, net_rx_mbps, net_tx_mbps,
# net_rx_pps, net_tx_pps. MB is 1048576 bytes. disk_busy_pct is the share of
# the interval the device had I/O in flight and can reach 100 on a device that
# still has headroom (any NVMe); read it with the IOPS columns.

pid=""
disk=""
iface=""
samples=0
while getopts "p:d:i:n:" option; do
    case "$option" in
        p) pid="$OPTARG" ;;
        d) disk="$OPTARG" ;;
        i) iface="$OPTARG" ;;
        n) samples="$OPTARG" ;;
        *) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
    esac
done

if [ -n "$disk" ] && ! awk -v d="$disk" '$3 == d { found = 1 } END { exit !found }' /proc/diskstats; then
    echo "sysmon.sh: disk '$disk' is not in /proc/diskstats; names there: $(awk '{ printf "%s ", $3 }' /proc/diskstats)" >&2
    exit 2
fi
if [ -n "$iface" ] && ! grep -q "^ *$iface:" /proc/net/dev; then
    echo "sysmon.sh: interface '$iface' is not in /proc/net/dev; names there: $(awk -F'[: ]+' 'NR > 2 { printf "%s ", $2 }' /proc/net/dev)" >&2
    exit 2
fi

exec awk -v pid="$pid" -v disk="$disk" -v iface="$iface" -v samples="$samples" '
function now(    line) {
    "date +%s.%N" | getline line
    close("date +%s.%N")
    return line + 0
}
function readCpu(    line, f, name) {
    cpuCount = 0
    while ((getline line < "/proc/stat") > 0) {
        if (line !~ /^cpu[0-9]* /) continue
        split(line, f, " ")
        name = f[1]
        cpuNames[cpuCount++] = name
        busy[name] = f[2] + f[3] + f[4] + f[7] + f[8] + f[9]
        total[name] = busy[name] + f[5] + f[6]
    }
    close("/proc/stat")
}
function readMem(    line, f) {
    while ((getline line < "/proc/meminfo") > 0) {
        split(line, f, " ")
        if (f[1] == "MemAvailable:") memAvailable = f[2]
        else if (f[1] == "Cached:") cached = f[2]
        else if (f[1] == "SwapTotal:") swapTotal = f[2]
        else if (f[1] == "SwapFree:") swapFree = f[2]
    }
    close("/proc/meminfo")
}
function readRss(    line, f, path) {
    rss = ""
    if (pid == "") return
    path = "/proc/" pid "/status"
    while ((getline line < path) > 0) {
        split(line, f, " ")
        if (f[1] == "VmRSS:") rss = sprintf("%.1f", f[2] / 1024)
    }
    close(path)
}
function readDisk(    line, f) {
    if (disk == "") return
    while ((getline line < "/proc/diskstats") > 0) {
        split(line, f, " ")
        if (f[3] != disk) continue
        reads = f[4]; readSectors = f[6]
        writes = f[8]; writeSectors = f[10]
        busyMs = f[13]
    }
    close("/proc/diskstats")
}
function readNet(    line, f) {
    if (iface == "") return
    while ((getline line < "/proc/net/dev") > 0) {
        sub(/:/, " ", line)
        split(line, f, " ")
        if (f[1] != iface) continue
        rxBytes = f[2]; rxPackets = f[3]
        txBytes = f[10]; txPackets = f[11]
    }
    close("/proc/net/dev")
}
function pct(name) {
    if (total[name] == previousTotal[name]) return "0.0"
    return sprintf("%.1f", 100 * (busy[name] - previousBusy[name]) / (total[name] - previousTotal[name]))
}
function rate(current, previous, scale) {
    return sprintf("%.2f", (current - previous) / elapsed / scale)
}
function remember(    i, name) {
    for (i = 0; i < cpuCount; i++) {
        name = cpuNames[i]
        previousBusy[name] = busy[name]; previousTotal[name] = total[name]
    }
    previous["reads"] = reads; previous["readSectors"] = readSectors
    previous["writes"] = writes; previous["writeSectors"] = writeSectors
    previous["busyMs"] = busyMs
    previous["rxBytes"] = rxBytes; previous["rxPackets"] = rxPackets
    previous["txBytes"] = txBytes; previous["txPackets"] = txPackets
}
BEGIN {
    readCpu()
    header = "epoch_s,cpu_pct"
    for (i = 1; i < cpuCount; i++) header = header "," cpuNames[i] "_pct"
    header = header ",mem_available_mb,cached_mb,swap_used_mb,rss_mb"
    header = header ",disk_read_mbps,disk_write_mbps,disk_read_iops,disk_write_iops,disk_busy_pct"
    header = header ",net_rx_mbps,net_tx_mbps,net_rx_pps,net_tx_pps"
    print header
    fflush()
    # The first reading is only the baseline for the first interval.
    readCpu(); readDisk(); readNet()
    remember()
    lastTime = now()
    count = 0
    while (samples == 0 || count < samples) {
        system("sleep 1")
        sampleTime = now()
        elapsed = sampleTime - lastTime
        lastTime = sampleTime
        if (elapsed <= 0) continue
        readCpu(); readMem(); readRss(); readDisk(); readNet()
        line = sprintf("%.0f,%s", sampleTime, pct("cpu"))
        for (i = 1; i < cpuCount; i++) line = line "," pct(cpuNames[i])
        line = line sprintf(",%.1f,%.1f,%.1f,%s", memAvailable / 1024, cached / 1024, (swapTotal - swapFree) / 1024, rss)
        if (disk != "") {
            line = line "," rate(readSectors, previous["readSectors"], 2048) "," rate(writeSectors, previous["writeSectors"], 2048)
            line = line "," rate(reads, previous["reads"], 1) "," rate(writes, previous["writes"], 1)
            line = line sprintf(",%.1f", 100 * (busyMs - previous["busyMs"]) / (elapsed * 1000))
        } else line = line ",,,,,"
        if (iface != "") {
            line = line "," rate(rxBytes, previous["rxBytes"], 1048576) "," rate(txBytes, previous["txBytes"], 1048576)
            line = line "," rate(rxPackets, previous["rxPackets"], 1) "," rate(txPackets, previous["txPackets"], 1)
        } else line = line ",,,,"
        print line
        fflush()
        remember()
        count++
    }
}'
