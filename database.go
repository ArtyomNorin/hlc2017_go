package main

import (
	"bufio"
	"fmt"
	"github.com/valyala/fasthttp"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var notFoundResponse = []byte(`HTTP/1.1 404 Not Found
Content-Length: 9
Content-Type: text/plain
Connection: Keep-Alive

Not Found`)

var badRequestResponse = []byte(`HTTP/1.1 400 Not Found
Content-Length: 11
Content-Type: text/plain
Connection: Keep-Alive

Bad Request`)

var emptyVisitsResponse = []byte(`HTTP/1.1 200 OK
Content-Length: 14
Content-Type: application/json
Connection: Keep-Alive

{"visits": []}`)

var emptyAvgResponse = []byte(`HTTP/1.1 200 OK
Content-Length: 10
Content-Type: application/json
Connection: Keep-Alive

{"avg": 0}`)

type DataBase struct {
	Users              []*User
	Locations          []*Location
	Visits             []*Visit
	EntityBufferPool   sync.Pool
	TimeDataGeneration time.Time
	IsTrain bool
}

func (db *DataBase) PrintStats() {
	fmt.Println(fmt.Sprintf("Count users: %d", len(db.Users)))
	fmt.Println(fmt.Sprintf("Count locations: %d", len(db.Locations)))
	fmt.Println(fmt.Sprintf("Count visits: %d", len(db.Visits)))
}

func (db *DataBase) SortIndexes() {
	for _, user := range db.Users {
		sort.Slice(user.VisitsIndex, func(i, j int) bool {
			return user.VisitsIndex[i].VisitedAt < user.VisitsIndex[j].VisitedAt
		})
	}
}

func (db *DataBase) GetUser(id int, responseBuffer []byte) []byte {
	if len(db.Users) < id {
		return notFoundResponse
	}

	entityBuffer := db.EntityBufferPool.Get().([]byte)
	entityBuffer = entityBuffer[:0]
	entityBuffer = db.Users[id-1].Serialize(entityBuffer)

	responseBuffer = append(responseBuffer, `HTTP/1.1 200 OK
Content-Length: `...)
	responseBuffer = fasthttp.AppendUint(responseBuffer, len(entityBuffer))
	responseBuffer = append(responseBuffer, `
Content-Type: application/json
Connection: Keep-Alive

`...)
	responseBuffer = append(responseBuffer, entityBuffer...)

	db.EntityBufferPool.Put(entityBuffer)

	return responseBuffer
}

func (db *DataBase) GetLocation(id int, responseBuffer []byte) []byte {
	if len(db.Locations) < id {
		return notFoundResponse
	}

	entityBuffer := db.EntityBufferPool.Get().([]byte)
	entityBuffer = entityBuffer[:0]
	entityBuffer = db.Locations[id-1].Serialize(entityBuffer)

	responseBuffer = append(responseBuffer, `HTTP/1.1 200 OK
Content-Length: `...)
	responseBuffer = fasthttp.AppendUint(responseBuffer, len(entityBuffer))
	responseBuffer = append(responseBuffer, `
Content-Type: application/json
Connection: Keep-Alive

`...)
	responseBuffer = append(responseBuffer, entityBuffer...)

	db.EntityBufferPool.Put(entityBuffer)

	return responseBuffer
}

func (db *DataBase) GetVisit(id int, responseBuffer []byte) []byte {
	if len(db.Visits) < id {
		return notFoundResponse
	}

	entityBuffer := db.EntityBufferPool.Get().([]byte)
	entityBuffer = entityBuffer[:0]
	entityBuffer = db.Visits[id-1].Serialize(entityBuffer)

	responseBuffer = append(responseBuffer, `HTTP/1.1 200 OK
Content-Length: `...)
	responseBuffer = fasthttp.AppendUint(responseBuffer, len(entityBuffer))
	responseBuffer = append(responseBuffer, `
Content-Type: application/json
Connection: Keep-Alive

`...)
	responseBuffer = append(responseBuffer, entityBuffer...)

	db.EntityBufferPool.Put(entityBuffer)

	return responseBuffer
}

func (db *DataBase) GetVisitedPlaces(id int, responseBuffer []byte, request *Request) []byte {
	if len(db.Users) < id {
		return notFoundResponse
	}

	fromDate := 0
	toDate := 0
	country := ""
	toDistance := 0

	var err error

	if fromDateReceived, isExist := request.Query["fromDate"]; isExist {
		if len(fromDateReceived) == 0 {
			return badRequestResponse
		}

		fromDate, err = strconv.Atoi(fromDateReceived);
		if err != nil {
			return badRequestResponse
		}
	}

	if toDateReceived, isExist := request.Query["toDate"]; isExist {
		if len(toDateReceived) == 0 {
			return badRequestResponse
		}

		toDate, err = strconv.Atoi(toDateReceived);
		if err != nil {
			return badRequestResponse
		}
	}

	if toDistanceReceived, isExist := request.Query["toDistance"]; isExist {
		if len(toDistanceReceived) == 0 {
			return badRequestResponse
		}

		toDistance, err = strconv.Atoi(toDistanceReceived);
		if err != nil {
			return badRequestResponse
		}
	}

	if countryReceived, isExist := request.Query["country"]; isExist {
		if len(countryReceived) == 0 {
			return badRequestResponse
		}

		country, err = url.QueryUnescape(countryReceived);
	}

	visits := db.Users[id-1].VisitsIndex

	if len(visits) == 0 {
		return emptyVisitsResponse
	}

	entityBuffer := db.EntityBufferPool.Get().([]byte)
	entityBuffer = entityBuffer[:0]

	entityBuffer = append(entityBuffer, `{"visits": [`...)

	for _, visit := range visits {
		if fromDate != 0 && visit.VisitedAt < fromDate {
			continue
		}

		if toDate != 0 && visit.VisitedAt > toDate {
			continue
		}

		if country != "" && visit.Location.Country != country {
			continue
		}

		if toDistance != 0 && visit.Location.Distance >= uint32(toDistance) {
			continue
		}

		entityBuffer = visit.SerializeVisited(entityBuffer)
		entityBuffer = append(entityBuffer, ',')
	}

	if len(entityBuffer) != 12 {
		entityBuffer = entityBuffer[:len(entityBuffer)-1]
	}
	entityBuffer = append(entityBuffer, `]}`...)

	responseBuffer = append(responseBuffer, `HTTP/1.1 200 OK
Content-Length: `...)
	responseBuffer = fasthttp.AppendUint(responseBuffer, len(entityBuffer))
	responseBuffer = append(responseBuffer, `
Content-Type: application/json
Connection: Keep-Alive

`...)

	responseBuffer = append(responseBuffer, entityBuffer...)

	db.EntityBufferPool.Put(entityBuffer)

	return responseBuffer
}

func (db *DataBase) GetAvgMark(id int, responseBuffer []byte, request *Request) []byte {
	if len(db.Locations) < id {
		return notFoundResponse
	}

	fromDate := 0
	toDate := 0
	gender := ""
	fromAge := 0
	toAge := 0

	var err error

	if fromDateReceived, isExist := request.Query["fromDate"]; isExist {
		if len(fromDateReceived) == 0 {
			return badRequestResponse
		}

		fromDate, err = strconv.Atoi(fromDateReceived);
		if err != nil {
			return badRequestResponse
		}
	}

	if toDateReceived, isExist := request.Query["toDate"]; isExist {
		if len(toDateReceived) == 0 {
			return badRequestResponse
		}

		toDate, err = strconv.Atoi(toDateReceived);
		if err != nil {
			return badRequestResponse
		}
	}

	if fromAgeReceived, isExist := request.Query["fromAge"]; isExist {
		if len(fromAgeReceived) == 0 {
			return badRequestResponse
		}

		fromAge, err = strconv.Atoi(fromAgeReceived);
		if err != nil {
			return badRequestResponse
		}
	}

	if toAgeReceived, isExist := request.Query["toAge"]; isExist {
		if len(toAgeReceived) == 0 {
			return badRequestResponse
		}

		toAge, err = strconv.Atoi(toAgeReceived);
		if err != nil {
			return badRequestResponse
		}
	}

	if genderReceived, isExist := request.Query["gender"]; isExist {
		if len(genderReceived) == 0 || (genderReceived != "m" && genderReceived != "f") {
			return badRequestResponse
		}

		gender = genderReceived
	}

	visits := db.Locations[id-1].VisitsIndex

	if len(visits) == 0 {
		return emptyAvgResponse
	}

	entityBuffer := db.EntityBufferPool.Get().([]byte)
	entityBuffer = entityBuffer[:0]

	entityBuffer = append(entityBuffer, `{"avg": `...)

	countVisits := 0
	sumOfMarks := 0

	for _, visit := range visits {
		if fromDate != 0 && visit.VisitedAt < fromDate {
			continue
		}

		if toDate != 0 && visit.VisitedAt > toDate {
			continue
		}

		if gender != "" && visit.User.Gender != gender {
			continue
		}

		if fromAge != 0 && int(db.TimeDataGeneration.AddDate(-fromAge, 0, 0).Unix()) < visit.User.BirthDate {
			continue
		}

		if toAge != 0 && int(db.TimeDataGeneration.AddDate(-toAge, 0, 0).Unix()) > visit.User.BirthDate {
			continue
		}

		sumOfMarks += int(visit.Mark)
		countVisits++
	}

	if countVisits == 0 {
		db.EntityBufferPool.Put(entityBuffer)
		return emptyAvgResponse
	}

	entityBuffer = strconv.AppendFloat(entityBuffer, math.Round(float64(sumOfMarks)/float64(countVisits)*100000)/100000, 'e', 6, 32)
	entityBuffer = append(entityBuffer, '}')

	responseBuffer = append(responseBuffer, `HTTP/1.1 200 OK
Content-Length: `...)
	responseBuffer = fasthttp.AppendUint(responseBuffer, len(entityBuffer))
	responseBuffer = append(responseBuffer, `
Content-Type: application/json
Connection: Keep-Alive

`...)

	responseBuffer = append(responseBuffer, entityBuffer...)

	db.EntityBufferPool.Put(entityBuffer)

	return responseBuffer
}

func InitDatabase(dataPath string, pathToOptions string) (*DataBase, error) {
	database := new(DataBase)
	database.EntityBufferPool = sync.Pool{New: func() interface{} { return make([]byte, 0, 4096) }}

	file, _ := os.Open(pathToOptions)

	fileScanner := bufio.NewScanner(file)

	fileScanner.Scan()

	timeDataGeneration, err := strconv.Atoi(fileScanner.Text())

	if err != nil {
		return nil, err
	}

	fileScanner.Scan()

	if fileScanner.Text() == "1" {
		database.IsTrain = false
	} else {
		database.IsTrain = true
	}

	database.TimeDataGeneration = time.Unix(int64(timeDataGeneration), 0)

	if database.IsTrain {
		database.Users = make([]*User, 10062)
		database.Locations = make([]*Location, 7978)
		database.Visits = make([]*Visit, 100620)
	} else {
		/*database.Users = make([]*User, 1000074)
		database.Locations = make([]*Location, 761314)
		database.Visits = make([]*Visit, 10000740)*/

		database.Users = make([]*User, 1000058)
		database.Locations = make([]*Location, 763802)
		database.Visits = make([]*Visit, 10000580)
	}

	for userId := range database.Users {
		database.Users[userId] = new(User)
	}

	for locationId := range database.Locations {
		database.Locations[locationId] = new(Location)
	}

	for visitId := range database.Visits {
		database.Visits[visitId] = new(Visit)
	}

	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			if strings.Contains(path, "user") {
				err := ResetFile(path)

				if err != nil {
					return err
				}

				for ParseEntity() {
					id := uint32(GetIntValue("id"))

					database.Users[id-1].Id = id
					database.Users[id-1].BirthDate = GetIntValue("birth_date")
					database.Users[id-1].Email = GetStringValue("email")
					database.Users[id-1].FirstName = GetStringValue("first_name")
					database.Users[id-1].LastName = GetStringValue("last_name")
					database.Users[id-1].Gender = GetStringValue("gender")

				}

			} else if strings.Contains(path, "location") {
				err := ResetFile(path)

				if err != nil {
					return err
				}

				for ParseEntity() {
					id := uint32(GetIntValue("id"))

					database.Locations[id-1].Id = uint32(GetIntValue("id"))
					database.Locations[id-1].City = GetStringValue("city")
					database.Locations[id-1].Country = GetStringValue("country")
					database.Locations[id-1].Place = GetStringValue("place")
					database.Locations[id-1].Distance = uint32(GetIntValue("distance"))
				}
			} else if strings.Contains(path, "visit") {
				err := ResetFile(path)

				if err != nil {
					return err
				}

				for ParseEntity() {
					id := uint32(GetIntValue("id"))

					database.Visits[id-1].Id = uint32(GetIntValue("id"))
					database.Visits[id-1].Location = database.Locations[GetIntValue("location")-1]
					database.Visits[id-1].User = database.Users[GetIntValue("user")-1]
					database.Visits[id-1].Mark = int8(GetIntValue("mark"))
					database.Visits[id-1].VisitedAt = GetIntValue("visited_at")

					database.Visits[id-1].Location.VisitsIndex = append(database.Visits[id-1].Location.VisitsIndex, database.Visits[id-1])
					database.Visits[id-1].User.VisitsIndex = append(database.Visits[id-1].User.VisitsIndex, database.Visits[id-1])
				}
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return database, nil
}
