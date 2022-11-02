package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
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
	trips := getTrips(getStopTimes())

	http.HandleFunc("/schedules/", func(w http.ResponseWriter, r *http.Request) {
		route := strings.Split(r.URL.Path, "/")[2]
		resp := trips[route]
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			fmt.Println("json error", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("500 - Something bad happened!"))
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
	r := csv.NewReader(bufio.NewReader(f))
	head, err := r.Read()
	if err != nil {
		panic(err)
	}

	if head[0] != "trip_id" || head[3] != "stop_id" || head[1] != "arrival_time" || head[2] != "departure_time" {
		fmt.Println("stop_times.txt not in expected format:")
		for i, cell := range head {
			fmt.Println(i, cell)
		}
		panic(1)
	}

	stopTimes := make(map[string][]StopTime)
	var n int
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		trip := rec[0]
		st := StopTime{
			StopID:    rec[3],
			Arrival:   rec[1],
			Departure: rec[2],
		}
		stopTimes[trip] = append(stopTimes[trip], st)
		n++
	}
	end := time.Now()
	elapsed := end.Sub(start)

	fmt.Println("parsed", n, "stop times in", elapsed)
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
	r := csv.NewReader(bufio.NewReader(f))
	head, err := r.Read()
	if err != nil {
		panic(err)
	}

	if head[2] != "trip_id" || head[0] != "route_id" || head[1] != "service_id" {
		fmt.Println("trips.txt not in expected format:")
		for i, cell := range head {
			fmt.Println(i, cell)
		}
		panic(1)
	}

	var n int
	trips := make(map[string][]Trip)
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		route := rec[0]
		t := Trip{
			RouteID:   route,
			Schedules: stopTimes[rec[2]],
			TripID:    rec[2],
			ServiceID: rec[1],
		}
		trips[route] = append(trips[route], t)
		n++
	}
	end := time.Now()
	elapsed := end.Sub(start)

	fmt.Println("parsed", n, "trips in", elapsed)

	return trips
}
