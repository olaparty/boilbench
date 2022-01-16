package golas

import (
	"database/sql"
	"log"
	"reflect"

	"github.com/volatiletech/null/v8"
)

type UserTable struct{}

func (*UserTable) GetTableName() string {
	return "users"
}

var table *UserTable

type TableType interface {
	GetTableName() string
}

type ColumnType interface {
	GetColumnName() string
	GetValPointer() interface{}
	IsPrimaryKey() bool
	GetTableType() TableType
}

// Jet struct
type Jet struct {
	ID
	PilotID
	AirportID
	Name
	Color
	UUID
	Identifier
	Cargo
	Manifest
}

type Color struct {
	val null.String
}

func (c *Color) GetId() null.String {
	return c.val
}

func (c *Color) SetId(val null.String) {
	c.val = val
}

func (c *Color) GetColumnName() string {
	return "Color"
}

func (c *Color) IsPrimaryKey() bool {
	return true
}

func (c *Color) GetValPointer() interface{} {
	return &c.val
}

func (c *Color) GetTableType() TableType {
	return table
}

type Manifest struct {
	val []byte
}

func (c *Manifest) GetId() []byte {
	return c.val
}

func (c *Manifest) SetId(val []byte) {
	c.val = val
}

func (c *Manifest) GetColumnName() string {
	return "Manifest"
}

func (c *Manifest) IsPrimaryKey() bool {
	return true
}

func (c *Manifest) GetValPointer() interface{} {
	return &c.val
}

func (c *Manifest) GetTableType() TableType {
	return table
}

type Cargo struct {
	val []byte
}

func (c *Cargo) GetId() []byte {
	return c.val
}

func (c *Cargo) SetId(val []byte) {
	c.val = val
}

func (c *Cargo) GetColumnName() string {
	return "Cargo"
}

func (c *Cargo) IsPrimaryKey() bool {
	return true
}

func (c *Cargo) GetValPointer() interface{} {
	return &c.val
}

func (c *Cargo) GetTableType() TableType {
	return table
}

type Identifier struct {
	val string
}

func (c *Identifier) GetId() string {
	return c.val
}

func (c *Identifier) SetId(val string) {
	c.val = val
}

func (c *Identifier) GetColumnName() string {
	return "UUID"
}

func (c *Identifier) IsPrimaryKey() bool {
	return true
}

func (c *Identifier) GetValPointer() interface{} {
	return &c.val
}

func (c *Identifier) GetTableType() TableType {
	return table
}

type UUID struct {
	val string
}

func (c *UUID) GetId() string {
	return c.val
}

func (c *UUID) SetId(val string) {
	c.val = val
}

func (c *UUID) GetColumnName() string {
	return "UUID"
}

func (c *UUID) IsPrimaryKey() bool {
	return true
}

func (c *UUID) GetValPointer() interface{} {
	return &c.val
}

func (c *UUID) GetTableType() TableType {
	return table
}

type Name struct {
	val string
}

func (c *Name) GetId() string {
	return c.val
}

func (c *Name) SetId(val string) {
	c.val = val
}

func (c *Name) GetColumnName() string {
	return "id"
}

func (c *Name) IsPrimaryKey() bool {
	return true
}

func (c *Name) GetValPointer() interface{} {
	return &c.val
}

func (c *Name) GetTableType() TableType {
	return table
}

type AirportID struct {
	val int
}

func (c *AirportID) GetId() int {
	return c.val
}

func (c *AirportID) SetId(val int) {
	c.val = val
}

func (c *AirportID) GetColumnName() string {
	return "id"
}

func (c *AirportID) IsPrimaryKey() bool {
	return true
}

func (c *AirportID) GetValPointer() interface{} {
	return &c.val
}

func (c *AirportID) GetTableType() TableType {
	return table
}

type PilotID struct {
	val int
}

func (c *PilotID) GetId() int {
	return c.val
}

func (c *PilotID) SetId(val int) {
	c.val = val
}

func (c *PilotID) GetColumnName() string {
	return "id"
}

func (c *PilotID) IsPrimaryKey() bool {
	return true
}

func (c *PilotID) GetValPointer() interface{} {
	return &c.val
}

func (c *PilotID) GetTableType() TableType {
	return table
}

type ID struct {
	val int
}

func (c *ID) GetId() int {
	return c.val
}

func (c *ID) SetId(val int) {
	c.val = val
}

func (c *ID) GetColumnName() string {
	return "id"
}

func (c *ID) IsPrimaryKey() bool {
	return true
}

func (c *ID) GetValPointer() interface{} {
	return &c.val
}

func (c *ID) GetTableType() TableType {
	return table
}

func (j *Jet) GetColumnNames() string {
	return "id,pilot_id,airport_id,name,color,uuid,identifier,cargo,manifest"
}

func (j *Jet) GetPointers() []interface{} {
	return []interface{}{
		j.ID.GetValPointer(),
		j.PilotID.GetValPointer(),
		j.AirportID.GetValPointer(),
		j.Name.GetValPointer(),
		j.Color.GetValPointer(),
		j.UUID.GetValPointer(),
		j.Identifier.GetValPointer(),
		j.Cargo.GetValPointer(),
		j.Manifest.GetValPointer(),
	}
}

type PointerType[T any] interface {
	*T
}

type RowStruct interface {
	GetColumnNames() string
	GetPointers() []interface{}
}

var _connstr string

func Setup(connstr string) {
	_connstr = connstr
}

func Query[T any, PT PointerType[T]](db *sql.DB, query string) []*T {
	var result []*T
	var u *T

	rows, err2 := db.Query(query)

	if err2 != nil {
		log.Fatal(err2)
	}

	for rows.Next() {
		u = new(T)

		if rs, ok := interface{}(u).(RowStruct); ok {
			rs.GetColumnNames()
			data := rs.GetPointers()
			rows.Scan(data...)
			result = append(result, u)
		}
	}

	return result
}

func QueryReflect[T any, PT PointerType[T]](db *sql.DB, query string) []*T {
	var result []*T
	var u *T

	rows, err2 := db.Query(query)

	if err2 != nil {
		log.Fatal(err2)
	}

	for rows.Next() {
		u = new(T)
		data := StrutForScan(u)
		rows.Scan(data...)
		result = append(result, u)
	}

	return result
}

func StrutForScan[T any, PT PointerType[T]](u PT) (pointers []interface{}) {
	val := reflect.ValueOf(u).Elem()
	pointers = make([]interface{}, 0, val.NumField())
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		if f, ok := valueField.Addr().Interface().(ColumnType); ok {
			pointers = append(pointers, f.GetValPointer())
		}
	}
	return
}
