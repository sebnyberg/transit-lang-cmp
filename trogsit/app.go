package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type Trip struct {
	TripID    string     `json:"trip_id"`
	ServiceID string     `json:"service_id"`
	RouteID   string     `json:"route_id"`
	Schedules []StopTime `json:"schedules"`
}

type StopTime struct {
	StopID    string `json:"stop_id"`
	Arrival   string `json:"arrival_time"`
	Departure string `json:"departure_time"`
}

func main() {
	stopTimes := getStopTimes()
	trips := getTrips(stopTimes)

	http.HandleFunc("/schedules/", func(w http.ResponseWriter, r *http.Request) {
		route := strings.Split(r.URL.Path, "/")[2]
		resp := trips[route]
		w.Header().Set("Content-Type", "application/json")
		json_resp, err := json.Marshal(resp)
		if err != nil {
			fmt.Println("json error", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("500 - Something bad happened!"))
		} else {
			w.Write(json_resp)
		}
	})
	log.Fatal(http.ListenAndServe(":4000", nil))
}

func getStopTimes() map[string][]StopTime {
	fname := "../MBTA_GTFS/stop_times.txt"
	f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	start := time.Now()
	r := csv.NewReader(bufio.NewReaderSize(f, 1024*1024*8))
	records, err := r.ReadAll()
	if err != nil {
		panic(err)
	}

	if records[0][0] != "trip_id" || records[0][3] != "stop_id" || records[0][1] != "arrival_time" || records[0][2] != "departure_time" {
		fmt.Println("stop_times.txt not in expected format:")
		for i, cell := range records[0] {
			fmt.Println(i, cell)
		}
		panic(1)
	}

	stopTimes := make(map[string][]StopTime)
	for _, rec := range records[1:] {
		trip := rec[0]
		st := StopTime{
			StopID:    rec[3],
			Arrival:   rec[1],
			Departure: rec[2],
		}
		stopTimes[trip] = append(stopTimes[trip], st)
	}
	end := time.Now()
	elapsed := end.Sub(start)

	fmt.Println("parsed", len(records)-1, "stop times in", elapsed)
	return stopTimes
}

func getTrips(stopTimes map[string][]StopTime) map[string][]Trip {
	fname := "../MBTA_GTFS/trips.txt"
	f, err := os.OpenFile(fname, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	start := time.Now()
	r := csv.NewReader(bufio.NewReaderSize(f, 1024*1024*8))
	records, err := r.ReadAll()
	if err != nil {
		panic(err)
	}

	if records[0][2] != "trip_id" || records[0][0] != "route_id" || records[0][1] != "service_id" {
		fmt.Println("trips.txt not in expected format:")
		for i, cell := range records[0] {
			fmt.Println(i, cell)
		}
		panic(1)
	}

	trips := make(map[string][]Trip)
	for _, rec := range records[1:] {
		route := rec[0]
		t := Trip{
			RouteID:   route,
			Schedules: stopTimes[rec[2]],
			TripID:    rec[2],
			ServiceID: rec[1],
		}
		trips[route] = append(trips[route], t)
	}
	end := time.Now()
	elapsed := end.Sub(start)

	fmt.Println("parsed", len(records)-1, "trips in", elapsed)

	return trips
}
