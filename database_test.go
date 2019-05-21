package main

import "testing"

func BenchmarkDataBase_GetVisitedPlaces(b *testing.B) {
	database, err := InitDatabase("/home/artyomnorin/Projects/hlc2017_go/data/train/data", "/home/artyomnorin/Projects/hlc2017_go/data/train/options.txt")

	if err != nil {
		b.Fatal(err)
	}

	responseBuffer := make([]byte, 0, 4096)

	request := new(Request)
	request.Query = map[string]string{}

	request.Query["toDistance"] = "49"
	request.Query["toDate"] = "1397433600"
	request.Query["fromDate"] = "1189209600"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		database.GetVisitedPlaces(752, responseBuffer, request)
		responseBuffer = responseBuffer[:0]
	}
}

func BenchmarkDataBase_GetAvgMark(b *testing.B) {
	database, err := InitDatabase("/home/artyomnorin/Projects/hlc2017_go/data/train/data", "/home/artyomnorin/Projects/hlc2017_go/data/train/options.txt")

	if err != nil {
		b.Fatal(err)
	}

	responseBuffer := make([]byte, 0, 4096)

	request := new(Request)
	request.Query = map[string]string{}

	request.Query["gender"] = "m"
	request.Query["fromAge"] = "4"
	request.Query["fromDate"] = "1453680000"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		database.GetAvgMark(752, responseBuffer, request)
		responseBuffer = responseBuffer[:0]
	}
}

func BenchmarkDataBase_GetUser(b *testing.B) {
	database, err := InitDatabase("/home/artyomnorin/Projects/hlc2017_go/data/train/data", "/home/artyomnorin/Projects/hlc2017_go/data/train/options.txt")

	if err != nil {
		b.Fatal(err)
	}

	responseBuffer := make([]byte, 0, 4096)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		database.GetUser(752, responseBuffer)
		responseBuffer = responseBuffer[:0]
	}
}