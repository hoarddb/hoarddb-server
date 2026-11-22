package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// checkEncryption inspects every node of the cluster: the TLS session a
// connection gets and a plaintext probe (probeNodeEncryption), then a real
// login through the load client's own dial, so the path a leg uses is the one
// proved. hoardDB reports nothing about a connection's encryption from the
// server side, so the proof is the client's session plus the refusal of plaintext.
func (h *hoardEngine) checkEncryption(_ context.Context) ([]tlsFinding, error) {
	var findings []tlsFinding
	for _, addr := range h.nodes() {
		finding := probeNodeEncryption(addr, h.cfg.OpTimeout)
		if finding.Encrypted {
			conn, err := dialWire(addr, h.cfg.Password, h.cfg.OpTimeout)
			if err != nil {
				finding.Encrypted, finding.Detail = false, fmt.Sprintf("the load client cannot log in over its own dial: %v", err)
			} else {
				conn.close()
			}
		}
		findings = append(findings, finding)
	}
	return findings, nil
}

// transportSecurityTotals reads two counters of serverStatus: connections ever
// created, and TLS handshakes completed (summed over versions). A connection
// accepted without TLS raises the first and not the second.
func (m *mongoEngine) transportSecurityTotals(ctx context.Context) (created, encrypted int64, err error) {
	var status struct {
		Connections struct {
			TotalCreated int64 `bson:"totalCreated"`
		} `bson:"connections"`
		TransportSecurity map[string]int64 `bson:"transportSecurity"`
	}
	if err := m.client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status); err != nil {
		return 0, 0, fmt.Errorf("reading serverStatus: %w", err)
	}
	for _, handshakes := range status.TransportSecurity {
		encrypted += handshakes
	}
	return status.Connections.TotalCreated, encrypted, nil
}

// checkEncryption opens a second client with the load client's options, fills
// its pool, and compares what the server says it created with the TLS
// handshakes it counted: the server, not the client, reports the connections
// encrypted. Handshakes are counted a moment after a connection is accepted, so
// the comparison is retried for a few seconds.
func (m *mongoEngine) checkEncryption(ctx context.Context) ([]tlsFinding, error) {
	finding := tlsFinding{Node: m.target()}
	createdBefore, encryptedBefore, err := m.transportSecurityTotals(ctx)
	if err != nil {
		return nil, err
	}
	loadClient, err := newMongoEngine(ctx, m.cfg)
	if err != nil {
		finding.Detail = fmt.Sprintf("the load client cannot connect: %v", err)
		return []tlsFinding{finding}, nil
	}
	defer loadClient.close()
	var group sync.WaitGroup
	for range m.cfg.Conc {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = loadClient.client.Ping(ctx, nil) // a failure leaves fewer connections, which the counts below show
		}()
	}
	group.Wait()
	var created, encrypted int64
	for range 10 {
		createdAfter, encryptedAfter, err := m.transportSecurityTotals(ctx)
		if err != nil {
			return nil, err
		}
		created, encrypted = createdAfter-createdBefore, encryptedAfter-encryptedBefore
		if created > 0 && encrypted >= created {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	finding.Encrypted = created > 0 && encrypted >= created
	finding.Detail = fmt.Sprintf("the server created %d connections while the load client connected and counted %d TLS handshakes (tls=%t in the client options)", created, encrypted, m.cfg.MongoTLS)
	return []tlsFinding{finding}, nil
}

// checkEncryption holds up to -conc connections to each server and asks the
// server, per connection, for the cipher it negotiated: an empty Ssl_cipher is a
// connection in the clear.
func (m *mysqlEngine) checkEncryption(ctx context.Context) ([]tlsFinding, error) {
	pools := []*sql.DB{m.db}
	for _, replica := range m.replicas {
		pools = append(pools, replica.db)
	}
	var findings []tlsFinding
	for index, pool := range pools {
		finding, err := encryptedConnections(ctx, pool, m.cfg.mysqlDSNs()[index], m.cfg.Conc)
		if err != nil {
			return nil, err
		}
		findings = append(findings, finding)
	}
	return findings, nil
}

// encryptedConnections opens count connections from pool at once and reads each
// one's Ssl_cipher.
func encryptedConnections(ctx context.Context, pool *sql.DB, node string, count int) (tlsFinding, error) {
	finding := tlsFinding{Node: node}
	held := make([]*sql.Conn, 0, count)
	defer func() {
		for _, conn := range held {
			_ = conn.Close() // returning a checked connection to the pool; nothing to recover
		}
	}()
	plaintext, ciphers := 0, map[string]int{}
	for range count {
		conn, err := pool.Conn(ctx)
		if err != nil {
			finding.Detail = fmt.Sprintf("the load client cannot connect: %v", err)
			return finding, nil
		}
		held = append(held, conn)
		var name, cipher string
		if err := conn.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&name, &cipher); err != nil {
			return finding, fmt.Errorf("reading Ssl_cipher of a connection to %s: %w", node, err)
		}
		if cipher == "" {
			plaintext++
		}
		ciphers[cipher]++
	}
	finding.Encrypted = plaintext == 0
	finding.Detail = fmt.Sprintf("%d connections held, %d without a cipher; ciphers negotiated: %v", count, plaintext, ciphers)
	return finding, nil
}
