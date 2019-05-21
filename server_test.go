package main

import (
	"testing"
)

func BenchmarkAcquireRequest(b *testing.B) {
	database, err := InitDatabase("/home/artyomnorin/Projects/hlc2017_go/data/train/data", "/home/artyomnorin/Projects/hlc2017_go/data/train/options.txt")

	if err != nil {
		b.Fatal(err)
	}

	server := NewServer(database)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		server.acquireRequest([]byte(`GET /users/752/visits?toDistance=49&toDate=1397433600&fromDate=1189209600 HTTP/1.1
cache-control: no-cache
Postman-Token: dec6fe8e-cb4f-491a-9bde-3c183da723f1
User-Agent: PostmanRuntime/7.6.0
Accept: */*
Host: localhost:8080
accept-encoding: gzip, deflate
Connection: keep-alive

`))
	}
}
