package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xuri/excelize/v2"
)

// структура для парсинга файла с данными из ИМУС
type tableCounter struct {
	gas_meter_id    string
	channel_id      string
	fabric_num      string
	gas_meter_model string
	type_name       string
	pc_id           string
	pc_name         string
	orig_id         string
	id_1c           string
}

// струтура для парсинга файла с данными из 1С Регионгаз
type table1C struct {
	id           string
	obj_conn     string
	distr_conn   string
	consumer     string
	distr_consum string
	identifier   string
	indications  string
	date_indic   string
	counter_numb string
}

/*
func stringToFloat(s string) float64 {
    f, err := strconv.ParseFloat(s, 64)
    if err != nil {
        panic(err)
    }
    return f
}

func stringToInt(s string) int {
    s = strings.TrimSuffix(s, ".0")
    i, err := strconv.Atoi(s)
    if err != nil {
        panic(err)
    }
    return i
}
*/

// функция для создания рабочих таблиц в SQLite
func createTable(db *sql.DB) {

	sqlStmt := `
	DROP TABLE IF EXISTS rayon;

	DROP TABLE IF EXISTS xx1C;
	
	DROP TABLE IF EXISTS xxCounter;
	
	CREATE TABLE IF NOT EXISTS rayon (
		id INTEGER,
		name TEXT,
		turg INTEGER
	);
	
	CREATE TABLE IF NOT EXISTS xx1C (
		id TEXT,
		obj_conn TEXT,
		distr_conn TEXT,
		consumer TEXT,
		distr_consum TEXT,
		identifier TEXT,
		indications TEXT,
		date_indic TEXT,
		counter_numb TEXT
	);
	
	CREATE TABLE IF NOT EXISTS xxCounter (
		gas_meter_id TEXT,
		channel_id TEXT,
		fabric_num TEXT,
		gas_meter_model TEXT,
		type_name TEXT,
		pc_id TEXT,
		pc_name TEXT,
		orig_id TEXT,
		id_1c TEXT
	);
	
	INSERT INTO rayon (id,name,turg) VALUES
		(1,'Краснояружский',4),
		(2,'Грайворонский',4),
		(3,'Ивнянский',8),
		(4,'Ракитянский',4),
		(5,'Борисовский',4),
		(6,'Прохоровский',8),
		(7,'Яковлевский',8),
		(8,'Белгородский',3),
		(9,'Губкинский',7),
		(10,'Корочанский',9),
		(11,'Шебекинский',9),
		(12,'Старооскольский',7),
		(13,'Чернянский',6),
		(14,'Новооскольский',6),
		(15,'Волоконовский',6),
		(16,'Валуйский',5),
		(17,'Красненский',1),
		(18,'Красногвардейский',1),
		(19,'Алексеевский',1),
		(20,'Вейделевский',5),
		(21,'Ровеньский',5),
		(22,'Белгород',2);
  `
	_, err := db.Exec(sqlStmt)
	if err != nil {
		fmt.Println(err)
	}
}

func findReplace(filepath string) {

	stringNeeded := "\t" + string(34)
	stringToReplace := "\t"
	file, err := os.Open(filepath)

	if err != nil {
		fmt.Println(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		text := scanner.Text()
		text = strings.Replace(text, stringNeeded, stringToReplace, -1)
		lines = append(lines, text)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	err = os.WriteFile(filepath, []byte(strings.Join(lines, "\r\n")), 0644)
	if err != nil {
		log.Fatalln(err)
	}

}

// функция для парсинга файла CSV
// Входные параметры: название файла и символ разделителя
// Выход:
func parseCsv(filepath string, comma byte, position byte) ([][]string, []string) {

	file, err := os.Open(filepath)

	if err != nil {
		fmt.Println(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = rune(comma)
	reader.LazyQuotes = true

	var records [][]string
	i := 0
	for {

		i++
		record, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println(err)
		}

		records = append(records, record)
	}

	fmt.Println("Импорт данных из файла ", filepath, ". Загружено записей:", i)

	return records[position:], records[0]
}

func parseCounter(records [][]string) []tableCounter {
	var recCounter []tableCounter
	for _, record := range records {
		recCount := tableCounter{
			gas_meter_id:    record[0],
			channel_id:      record[1],
			fabric_num:      record[2],
			gas_meter_model: record[3],
			type_name:       record[4],
			pc_id:           record[5],
			pc_name:         record[6],
			orig_id:         record[7],
			id_1c:           record[7],
		}
		recCounter = append(recCounter, recCount)
	}
	return recCounter
}

func parse1C(records [][]string) []table1C {
	var recCounter []table1C
	var space string
	var recCount table1C
	for _, record := range records {
		if len(record) == 9 {
			recCount = table1C{
				id:           record[0],
				obj_conn:     record[1],
				distr_conn:   record[2],
				consumer:     record[3],
				distr_consum: record[4],
				identifier:   record[5],
				indications:  record[6],
				date_indic:   record[7],
				counter_numb: record[8],
			}
		} else {
			recCount = table1C{
				id:           record[0],
				obj_conn:     record[1],
				distr_conn:   record[2],
				consumer:     record[3],
				distr_consum: record[4],
				identifier:   record[5],
				indications:  space,
				date_indic:   space,
				counter_numb: space,
			}
		}
		recCounter = append(recCounter, recCount)
	}
	return recCounter
}

func openDatabase() *sql.DB {
	if _, err := os.Stat("importconsumer.db"); err == nil {
		// database exists
		db, err := sql.Open("sqlite3", "importconsumer.db")
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(" Opened database")
		}
		return db

	} else {
		// database does not exist so create a table and return the database
		db, err := sql.Open("sqlite3", "importconsumer.db")
		if err != nil {
			fmt.Println(err)
		}
		createTable(db)
		return db
	}
}

func insertDataCounter(db *sql.DB, recCounter []tableCounter) {
	for _, recCount := range recCounter {
		//
		sqlStmt := `
  INSERT INTO xxCounter(gas_meter_id,channel_id,fabric_num,gas_meter_model,type_name,pc_id,pc_name,orig_id,id_1c) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
  `

		_, err := db.Exec(sqlStmt, recCount.gas_meter_id, recCount.channel_id, recCount.fabric_num, recCount.gas_meter_model, recCount.type_name, recCount.pc_id, recCount.pc_name, recCount.orig_id, recCount.id_1c)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func insertData1C(db *sql.DB, recCounter []table1C) {
	for _, recCount := range recCounter {
		//
		sqlStmt := `
  INSERT INTO xx1C(id, obj_conn, distr_conn, consumer, distr_consum, identifier, indications, date_indic, counter_numb) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
  `
		//indic_str := strings.Join(strings.Fields(recCount.indications), "")
		indic_str := strings.ReplaceAll(recCount.indications, ",", "")
		_, err := db.Exec(sqlStmt, recCount.id, recCount.obj_conn, recCount.distr_conn, recCount.consumer, recCount.distr_consum, recCount.identifier, indic_str, recCount.date_indic, recCount.counter_numb)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func changeDB(db *sql.DB) {

	// обрезаем идентификатор, по которому будем связывать таблицы
	_, err := db.Exec("UPDATE xxCounter SET id_1c = substr(id_1c,1,36)")
	if err != nil {
		fmt.Println(err)
	}

	// создаем представление связыванием 3-х таблиц
	sqlStmt := `
	CREATE VIEW izmer AS
    SELECT xx1C.id AS id_ais,
           xx1C.consumer AS FIO,
           xx1C.obj_conn AS adress,
           xxCounter.gas_meter_model AS typeCounter,
           xxCounter.fabric_num AS nCounter,
           xxCounter.gas_meter_id AS LS_GAS,
           rayon.turg AS turg_name,
           rayon.id AS rn,
           xxCounter.id_1C AS equipment_uuid,
           xx1C.indications AS past_indications,
           xx1C.date_indic AS date_indications,
           xxCounter.pc_name AS fio_adres
      FROM xxCounter
           LEFT JOIN
           (
               xx1C
               LEFT JOIN
               rayon ON xx1C.distr_conn = rayon.name
           )
           ON xxCounter.id_1c = xx1C.identifier;
  `
	_, err = db.Exec(sqlStmt)
	if err != nil {
		fmt.Println(err)
	}

	// копируем данные из представления в новую таблицу
	_, err = db.Exec("CREATE TABLE izmeritel AS SELECT * FROM izmer")
	if err != nil {
		fmt.Println(err)
	}

	// обновляем в таблице информацию по ФИО и адресу из вспомогательного поля
	_, err = db.Exec("UPDATE izmeritel SET id_ais='',FIO = fio_adres,adress = fio_adres,turg_name = 0,rn = 0,past_indications='',date_indications='' WHERE rn IS NULL")
	if err != nil {
		fmt.Println(err)
	}

	// изменяем в таблице номер района
	_, err = db.Exec("UPDATE izmeritel SET rn = 8 WHERE rn == 22")
	if err != nil {
		fmt.Println(err)
	}

	// изменяем в таблице номер ТУРГа
	_, err = db.Exec("UPDATE izmeritel SET turg_name = 3 WHERE turg_name == 2")
	if err != nil {
		fmt.Println(err)
	}

	// создаем представление для поиска повторяющихся записей
	sqlStmt = `
	CREATE VIEW replay AS
    SELECT max(izmeritel.id_ais) AS id_ais,
           max(izmeritel.LS_GAS) AS id_replay,
           Count(izmeritel.LS_GAS) AS ls_replay
      FROM izmeritel
     GROUP BY izmeritel.LS_GAS
    HAVING ( ( (Count(izmeritel.LS_GAS) ) > 1) );
  	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		fmt.Println(err)
	}

	// удаляем повторяющиеся записи
	_, err = db.Exec("DELETE FROM izmeritel WHERE id_ais in (SELECT id_ais FROM replay)")
	if err != nil {
		fmt.Println(err)
	}

}

func createXls(db *sql.DB) {

	frw, err := os.Create("izmeritel.csv")
	defer frw.Close()

	if err != nil {
		fmt.Println(err)
	}

	writer := csv.NewWriter(frw)
	writer.Comma = rune('|')
	defer writer.Flush()

	rowcsv := make([]string, 10)

	rowcsv[0] = "id_ais"
	rowcsv[1] = "fio"
	rowcsv[2] = "adress"
	rowcsv[3] = "typecounter"
	rowcsv[4] = "ncounter_real"
	rowcsv[5] = "ls_gas"
	rowcsv[6] = "equipment_uuid"
	rowcsv[7] = "id_rajon"
	rowcsv[8] = "amount"
	rowcsv[9] = "date_amount"

	if err := writer.Write(rowcsv); err != nil {
		fmt.Println(err)
	}

	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	f.SetCellValue("Sheet1", "A1", "id_ais")
	f.SetCellValue("Sheet1", "B1", "fio")
	f.SetCellValue("Sheet1", "C1", "adress")
	f.SetCellValue("Sheet1", "D1", "typeCounter")
	f.SetCellValue("Sheet1", "E1", "ncounter_real")
	f.SetCellValue("Sheet1", "F1", "ls_gas")
	f.SetCellValue("Sheet1", "G1", "equipment_uuid")
	f.SetCellValue("Sheet1", "H1", "id_rajon")
	f.SetCellValue("Sheet1", "I1", "amount")
	f.SetCellValue("Sheet1", "J1", "date_amount")

	i := 1
	rows, err := db.Query("SELECT * from izmeritel")
	if err == nil {
		for rows.Next() {

			var id, fio, adr, typeC, nC, ls, pind, dind, uuid, fa string
			var turg, rn int
			var ia string
			rows.Scan(&id, &fio, &adr, &typeC, &nC, &ls, &turg, &rn, &uuid, &pind, &dind, &fa)
			i++

			rowcsv[0] = id
			rowcsv[1] = fio
			rowcsv[2] = adr
			rowcsv[3] = typeC
			rowcsv[4] = nC
			rowcsv[5] = ls
			rowcsv[6] = uuid
			rowcsv[7] = strconv.Itoa(rn)
			rowcsv[8] = pind
			rowcsv[9] = dind

			if err := writer.Write(rowcsv); err != nil {
				fmt.Println(err)
			}

			ia = strconv.Itoa(i)
			pok_ind, _ := strconv.ParseInt(pind, 10, 64)
			f.SetCellValue("Sheet1", "A"+ia, id)
			f.SetCellValue("Sheet1", "B"+ia, fio)
			f.SetCellValue("Sheet1", "C"+ia, adr)
			f.SetCellValue("Sheet1", "D"+ia, typeC)
			f.SetCellValue("Sheet1", "E"+ia, nC)
			f.SetCellValue("Sheet1", "F"+ia, ls)
			f.SetCellValue("Sheet1", "G"+ia, uuid)
			f.SetCellValue("Sheet1", "H"+ia, rn)
			if pok_ind > 0 {
				f.SetCellValue("Sheet1", "I"+ia, pok_ind)
			} else {
				f.SetCellValue("Sheet1", "I"+ia, pind)
			}
			f.SetCellValue("Sheet1", "J"+ia, dind)
		}
	} else {
		fmt.Println(err)
	}

	// Сохранить файл xlsx по данному пути
	if err := f.SaveAs("izmeritel.xlsx"); err != nil {
		fmt.Println(err)
	}
}

func readXLS(filepath string) ([][]string, []string) {
	f, err := excelize.OpenFile(filepath)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	var records [][]string
	sheetname := f.GetSheetName(f.GetActiveSheetIndex())
	records, err = f.GetRows(sheetname)
	if err != nil {
		fmt.Println(err)
	}
	i := len(records)
	/*for _, row := range records {
		for _, col := range row {
			fmt.Print(col, "\t")
		}
		fmt.Println()
	}*/
	fmt.Println("Импорт данных из файла ", filepath, ". Загружено записей:", i)
	return records[1:], records[0]
}

func Begin(db *sql.DB) error {
	stmt, err := db.Prepare(`BEGIN`)
	if err != nil {
		return err
	}
	if _, err = stmt.Exec(); err != nil {
		return err
	}
	return nil
}

func Commit(db *sql.DB) error {
	stmt, err := db.Prepare(`COMMIT`)
	if err != nil {
		return err
	}
	if _, err = stmt.Exec(); err != nil {
		return err
	}
	return nil
}

// Главная программа приложения
func main() {
	arguments := os.Args
	if len(arguments) < 3 {
		fmt.Println("В командной строке должно быть 2 аргумента: файл *.xlsx и файл *.csv")
		return
	}

	inputXLS := os.Args[1]
	inputCSV := os.Args[2]

	// удаляем, если существуют, выходные файлы
	err := os.Remove("izmeritel.csv")
	if err != nil {
		fmt.Println(err)
	}

	err = os.Remove("izmeritel.xlsx")
	if err != nil {
		fmt.Println(err)
	}

	// удаляем базу данных
	err = os.Remove("importconsumer.db")
	if err != nil {
		fmt.Println(err)
	}

	// заново создаем базу данных
	db := openDatabase()
	defer db.Close()

	findReplace(inputCSV)

	data, _ := parseCsv(inputCSV, '\t', 1)

	if err := Begin(db); err != nil {
		fmt.Println(err)
		//log.Fatal(err)
	}
	insertDataCounter(db, parseCounter(data))
	Commit(db)

	//data, _ = parseCsv("xxxx1C.csv", ';', 1)
	data, _ = readXLS(inputXLS)
	if err := Begin(db); err != nil {
		fmt.Println(err)
	}
	insertData1C(db, parse1C(data))
	Commit(db)

	changeDB(db)

	createXls(db)

	fmt.Println("Обработка завершена. Сформирован файл izmeritel.xlsx")

}
