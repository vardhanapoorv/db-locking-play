package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/bxcodec/faker/v3"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	start := time.Now()
	db, err := createDBConn()
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}
	defer db.Close()

	allocate_seat(db)

	duration := time.Since(start)
	fmt.Println("Time spent", duration)
	/*
		  numPassengers := 120 // Number of passengers to generate
			err = loadPassengers(db, numPassengers)
			if err != nil {
				fmt.Println("Error loading passengers:", err)
				return
			}

			fmt.Println("Passenger data loaded successfully.")
	*/
	/*
		numRows := 20    // Number of rows
		seatsPerRow := 6 // Number of seats per row
		err = loadSeats(db, numRows, seatsPerRow)
		if err != nil {
			fmt.Println("Error loading seats:", err)
			return
		}

		fmt.Println("Seats data loaded successfully.")
	*/
}

func createDBConn() (*sql.DB, error) {
	db, err := sql.Open("mysql", "stnduser:stnduser@tcp(127.0.0.1:16033)/testdbrep")
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func allocate_seat(db *sql.DB) error {
	query := "SELECT passenger_id, first_name, last_name FROM Passenger"

	// Execute the query
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var wg sync.WaitGroup

	// Iterate over the result set and print each passenger's information
	for rows.Next() {
		var passengerID int
		var firstName, lastName string

		err := rows.Scan(&passengerID, &firstName, &lastName)
		if err != nil {
			return err
		}

		fmt.Printf("Passenger ID: %d, Name: %s %s\n", passengerID, firstName, lastName)
		wg.Add(1)

		go func(passengerID int) {
			defer wg.Done()
			bookSeat(db, passengerID)
		}(passengerID)
	}

	wg.Wait()

	// Check for any errors during iteration
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func bookSeat(db *sql.DB, passengerID int) error {

	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Error beginning transaction: %v", err)
	}

	query := "SELECT seat_id FROM Seat where passenger_id is NULL order by seat_id limit 1 FOR UPDATE SKIP LOCKED"

	fmt.Println("dd", passengerID)
	// Execute the query
	row := tx.QueryRow(query)
	var seatID int

	// Scan for the values from the row
	if err := row.Scan(&seatID); err != nil {
		if err == sql.ErrNoRows {
			fmt.Println("No results found.")
		}
		return err
	}

	fmt.Println("Value", seatID)

	updateStmt := `
		UPDATE Seat
		SET passenger_id = ?
		WHERE seat_id = ?
	`

	// Execute the update statement for the specified seat ID
	result, err := tx.Exec(updateStmt, passengerID, seatID)
	if err != nil {
		fmt.Println(err)
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Updated %d rows for seat %d\n", rowsAffected, seatID)

	// Commit the transaction if all operations succeed
	err = tx.Commit()
	if err != nil {
		log.Fatalf("Error committing transaction: %v", err)
	}

	fmt.Println("Transaction committed successfully")

	return nil
}

func loadPassengers(db *sql.DB, numPassengers int) error {
	stmt, err := db.Prepare("INSERT INTO Passenger (first_name, last_name, email, phone, date_of_birth) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 0; i < numPassengers; i++ {
		firstName := faker.FirstName()
		lastName := faker.LastName()
		email := faker.Email()
		phone := faker.Phonenumber()
		dateOfBirth := faker.Date()
		_, err := stmt.Exec(firstName, lastName, email, phone, dateOfBirth)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadSeats(db *sql.DB, numRows, seatsPerRow int) error {
	stmt, err := db.Prepare("INSERT INTO Seat (seat_number, flight_id) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	seatLetters := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"} // Add more letters as needed

	for row := 1; row <= numRows; row++ {
		for _, letter := range seatLetters[:seatsPerRow] {
			seatNumber := fmt.Sprintf("%d-%s", row, letter)
			flightId := 1

			_, err := stmt.Exec(seatNumber, flightId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Time spent 325.983833ms - PLAIN FOR UPDATE
//  Time spent 86.873917ms - SKIP LOCKED
