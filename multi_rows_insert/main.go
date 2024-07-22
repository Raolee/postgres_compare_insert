package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/postgres"
)

const (
	totalRows = 600000 // 1 minute at 10,000 rows/second
	batchSize = 1000   // Multi Insert 는 1000줄로 제한이 있다고 가정
)

type Event struct {
	ID        int
	Timestamp time.Time
	EventData string
}

func main() {
	// Setup Gnomock Postgres
	p := postgres.Preset(
		postgres.WithUser("raol", "raol"),
		postgres.WithDatabase("testdb"),
	)
	container, err := gnomock.Start(p)
	if err != nil {
		log.Fatalf("failed to start gnomock container: %v", err)
	}
	defer gnomock.Stop(container)

	connString := fmt.Sprintf("postgres://raol:raol@%s/testdb?sslmode=disable", container.DefaultAddress())

	// Setup database connections
	ctx := context.Background()
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Fatalf("unable to parse connection string: %v", err)
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	defer pool.Close()

	// Create tables
	_, err = pool.Exec(ctx, `
		CREATE TABLE events_no_index (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMP NOT NULL,
			event_data TEXT NOT NULL
		);
		
		CREATE TABLE events_with_index (
			id SERIAL PRIMARY KEY,
			timestamp TIMESTAMP NOT NULL,
			event_data TEXT NOT NULL
		);
		CREATE INDEX idx_timestamp ON events_with_index (timestamp);
	`)
	if err != nil {
		log.Fatalf("failed to create tables: %v", err)
	}

	// Generate test data
	events := make([]Event, totalRows)
	for i := 0; i < totalRows; i++ {
		events[i] = Event{
			ID:        i,
			Timestamp: time.Now(),
			EventData: fmt.Sprintf("event_data_%d", rand.Intn(1000)),
		}
	}

	// Channels for each table
	noIndexChan := make(chan Event, batchSize*2)
	withIndexChan := make(chan Event, batchSize*2)

	////////////////////////////////
	noIndexWg := sync.WaitGroup{}
	noIndexWg.Add(1)
	go multiRowInsertWorker(ctx, &noIndexWg, pool, 8, "events_no_index", noIndexChan)

	startTime := time.Now()
	for _, event := range events {
		noIndexChan <- event
	}
	close(noIndexChan)
	noIndexWg.Wait()
	duration := time.Since(startTime)
	fmt.Printf("Total duration: %v\n", duration)
	////////////////////////////////

	withIndexWg := sync.WaitGroup{}
	withIndexWg.Add(1)
	go multiRowInsertWorker(ctx, &withIndexWg, pool, 8, "events_with_index", withIndexChan)

	startTime = time.Now()
	for _, event := range events {
		withIndexChan <- event
	}
	close(withIndexChan)
	withIndexWg.Wait()
	duration = time.Since(startTime)
	fmt.Printf("Total duration: %v\n", duration)

}

func multiRowInsertWorker(ctx context.Context, wg *sync.WaitGroup, pool *pgxpool.Pool, workerCount int, tableName string, eventChan <-chan Event) {
	defer wg.Done()

	for i := 0; i < workerCount; i++ {
		go func() {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				log.Fatalf("unable to acquire a connection: %v", err)
			}
			defer conn.Release()

			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			var events []Event

			execByMaxCount := 0
			byMaxCopyFromLen := 0
			var byMaxDurationMilliSec int64

			execByTickCount := 0
			byTickCopyFromLen := 0
			var byTickDurationMilliSec int64
			defer func() {
				fmt.Printf("execByMaxCount : %d, execByTickCount : %d\n", execByMaxCount, execByTickCount)
				fmt.Printf("avg byMaxCopyFromLen : %d, avg byTickCopyFromLen : %d\n", byMaxCopyFromLen/execByMaxCount, byTickCopyFromLen/execByTickCount)
				fmt.Printf("avg byMaxDuration : %dms, avg byTickDuration : %dms\n", byMaxDurationMilliSec/int64(execByMaxCount), byTickDurationMilliSec/int64(execByTickCount))
			}()

			for {
				select {
				case event, ok := <-eventChan:
					if !ok {
						// Channel closed, flush remaining events
						if len(events) > 0 {
							byMaxDurationMilliSec += multiRowsInsert(ctx, conn, tableName, events)
							execByMaxCount++
							byMaxCopyFromLen += len(events)
						}
						return
					}
					events = append(events, event)
					if len(events) >= batchSize {
						byMaxDurationMilliSec += multiRowsInsert(ctx, conn, tableName, events)
						execByMaxCount++
						byMaxCopyFromLen += len(events)
						events = nil
					}
				case <-ticker.C:
					if len(events) > 0 {
						byTickDurationMilliSec += multiRowsInsert(ctx, conn, tableName, events)
						execByTickCount++
						byTickCopyFromLen += len(events)
						events = nil
					}
				}
			}
		}()
	}
}

func multiRowsInsert(ctx context.Context, conn *pgxpool.Conn, tableName string, events []Event) int64 {

	start := time.Now().UnixNano()

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Fatalf("failed to begin transaction: %v", err)
	}

	valueStrings := make([]string, 0, len(events))
	valueArgs := make([]interface{}, 0, len(events)*2)
	for i, event := range events {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2))
		valueArgs = append(valueArgs, event.Timestamp, event.EventData)
	}

	stmt := fmt.Sprintf("INSERT INTO %s (timestamp, event_data) VALUES %s", tableName, strings.Join(valueStrings, ","))
	_, err = tx.Exec(ctx, stmt, valueArgs...)
	if err != nil {
		log.Fatalf("failed to insert: %v", err)
	}

	_ = tx.Commit(ctx)

	return (time.Now().UnixNano() - start) / 1000000
}
