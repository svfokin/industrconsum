package main

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

func listDrivers() {
	for _, driver := range sql.Drivers() {
		fmt.Printf("Driver: %v", driver)
	}
}

func openDatabase1() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite", "products.db")
	if err == nil {
		fmt.Println(" Opened database")
	}
	return
}
