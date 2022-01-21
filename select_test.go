package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/gobuffalo/pop/v6"

	"github.com/volatiletech/boilbench/golas"
	"github.com/volatiletech/boilbench/gorms"
	"github.com/volatiletech/boilbench/gorps"
	"github.com/volatiletech/boilbench/mimic"
	"github.com/volatiletech/boilbench/models"
	"github.com/volatiletech/boilbench/xorms"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
	"gopkg.in/gorp.v1"
	"gorm.io/gorm"
	"xorm.io/xorm"
)

func BenchmarkGORMSelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	gormdb, err := gorm.Open(gormMimicDialector, &gorm.Config{})
	if err != nil {
		panic(err)
	}

	b.Run("gorm", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorms.Jet
			err := gormdb.Find(&store).Error
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkGOLASelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("golas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store := golas.Query[golas.Jet](db, "select * from jets")
			if len(store) != 5 {
				b.Fatal(errors.New("gola load failed"))
			}
			store = nil
		}
	})
}

func BenchmarkGOLA2SelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("golas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store := golas.QueryReflect[golas.Jet](db, "select * from jets")
			if len(store) != 5 {
				b.Fatal(errors.New("gola load failed"))
			}
			store = nil
		}
	})
}

func BenchmarkGOLA3SelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("golas", func(b *testing.B) {
		var j *golas.Jet
		for i := 0; i < b.N; i++ {
			store := golas.QueryStruct(db, "select * from jets", j)
			if len(store) != 5 {
				b.Fatal(errors.New("gola load failed"))
			}
			store = nil
		}
	})
}

func BenchmarkGOLA4SelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("golas", func(b *testing.B) {
		var j *golas.Jet
		for i := 0; i < b.N; i++ {
			store := golas.QueryStruct(db, "select * from jets", j)
			if len(store) != 5 {
				b.Fatal(errors.New("gola load failed"))
			}

			objs := make([]golas.Jet, len(store))
			for i, j := range store {
				objs[i] = *j.(*golas.Jet)
			}
			objs = nil
			store = nil
		}
	})
}

func BenchmarkGOLA5SelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("golas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store := make([]*golas.Jet, 0, 8)

			golas.QueryFunc(db, "select * from jets", func(row *sql.Rows) {
				o := new(golas.Jet)
				row.Scan(
					o.ID.GetValPointer(),
					o.PilotID.GetValPointer(),
					o.AirportID.GetValPointer(),
					o.Name.GetValPointer(),
					o.Color.GetValPointer(),
					o.UUID.GetValPointer(),
					o.Identifier.GetValPointer(),
					o.Cargo.GetValPointer(),
					o.Manifest.GetValPointer(),
				)
				store = append(store, o)
			})

			if len(store) != 5 {
				b.Fatal(errors.New("gola load failed"))
			}
			store = nil
		}
	})
}

func BenchmarkGOLA6SelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("golas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			store := make([]*golas.Jet, 0, 8)
			var o golas.Jet
			golas.QueryFunc(db, "select * from jets", func(row *sql.Rows) {
				store = append(store, o.DoScan(row).(*golas.Jet))
			})

			if len(store) != 5 {
				b.Fatal(errors.New("gola load failed"))
			}
			store = nil
		}
	})
}

func BenchmarkGOLA7SelectAll(b *testing.B) {
	// using full reflection without generic
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("golas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var j golas.Jet
			store := golas.QueryStructReflect(db, "select * from jets", &j)
			if len(store) != 5 {
				b.Fatal(errors.New("gola load failed"))
			}

			objs := make([]*golas.Jet, len(store))
			for i, j := range store {
				objs[i] = j.(*golas.Jet)
			}
			objs = nil
			store = nil
		}
	})
}

func BenchmarkGORPSelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	gorpdb := &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}
	if err != nil {
		panic(err)
	}

	b.Run("gorp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorps.Jet
			_, err = gorpdb.Select(&store, "select * from jets")
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkXORMSelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	xormdb, err := xorm.NewEngine("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("xorm", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []xorms.Jet
			err = xormdb.Find(&store)
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkBoilSelectAll(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("boil", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			_, err = models.Jets().All(ctx, db)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPopSelectAll(b *testing.B) {
	dsn := "postgres://BenchmarkPopSelectAll"

	query := jetQuery()
	mimic.NewQueryDSN(dsn, query)

	popdb, err := pop.NewConnection(&pop.ConnectionDetails{Driver: "mimic", Dialect: "postgres", URL: dsn})
	if err != nil {
		panic(err)
	}

	err = popdb.Open()
	if err != nil {
		panic(err)
	}

	b.Run("pop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorps.Jet
			err = popdb.All(&store)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGORMSelectSubset(b *testing.B) {
	var store []gorms.Jet
	query := jetQuery()
	mimic.NewQuery(query)

	gormdb, err := gorm.Open(gormMimicDialector, &gorm.Config{})
	if err != nil {
		panic(err)
	}

	b.Run("gorm", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err = gormdb.Select("id, name, color, uuid, identifier, cargo, manifest").Find(&store).Error
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkGORPSelectSubset(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	gorpdb := &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}
	if err != nil {
		panic(err)
	}

	b.Run("gorp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorps.Jet
			_, err = gorpdb.Select(&store, `select id, name, color, uuid, identifier, cargo, manifest from "jets"`)
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkXORMSelectSubset(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	xormdb, err := xorm.NewEngine("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("xorm", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []xorms.Jet
			err = xormdb.Select("id, name, color, uuid, identifier, cargo, manifest").Find(&store)
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkBoilSelectSubset(b *testing.B) {
	query := jetQuery()
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("boil", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			_, err = models.Jets(qm.Select("id, name, color, uuid, identifier, cargo, manifest")).All(ctx, db)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPopSelectSubset(b *testing.B) {
	dsn := "postgres://BenchmarkPopSelectSubset"

	query := jetQuery()
	mimic.NewQueryDSN(dsn, query)

	popdb, err := pop.NewConnection(&pop.ConnectionDetails{Driver: "mimic", Dialect: "postgres", URL: dsn})
	if err != nil {
		panic(err)
	}

	err = popdb.Open()
	if err != nil {
		panic(err)
	}

	b.Run("pop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorps.Jet
			err = popdb.Select("id, name, color, uuid, identifier, cargo, manifest").All(&store)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGORMSelectComplex(b *testing.B) {
	query := jetQuery()
	query.NumInput = -1
	mimic.NewQuery(query)

	gormdb, err := gorm.Open(gormMimicDialector, &gorm.Config{})
	if err != nil {
		panic(err)
	}

	b.Run("gorm", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorms.Jet
			err = gormdb.Where("id > ?", 1).
				Where("name <> ?", "thing").
				Limit(1).
				Group("id").
				Offset(1).
				Select("id, name, color, uuid, identifier, cargo, manifest").
				Find(&store).Error
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkGORPSelectComplex(b *testing.B) {
	query := jetQuery()
	query.NumInput = -1
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	gorpdb := &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}
	if err != nil {
		panic(err)
	}

	b.Run("gorp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorps.Jet
			_, err = gorpdb.Select(&store, `
			select id, name, color, uuid, identifier, cargo, manifest from "jets"
			where id > $1 and name <> $2 group by "id" offset $3 limit $4
		`, 1, "thing", 1, 1)
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkXORMSelectComplex(b *testing.B) {
	query := jetQuery()
	query.NumInput = -1
	mimic.NewQuery(query)

	xormdb, err := xorm.NewEngine("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("xorm", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []xorms.Jet
			err = xormdb.
				Select("id, name, color, uuid, identifier, cargo, manifest").
				Where("id > ?", 1).
				Where("name <> ?", "thing").
				Limit(1, 1).
				GroupBy("id").
				Find(&store)
			if err != nil {
				b.Fatal(err)
			}
			store = nil
		}
	})
}

func BenchmarkBoilSelectComplex(b *testing.B) {
	query := jetQuery()
	query.NumInput = -1
	mimic.NewQuery(query)

	db, err := sql.Open("mimic", "")
	if err != nil {
		panic(err)
	}

	b.Run("boil", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			_, err = models.Jets(
				qm.Select("id, name, color, uuid, identifier, cargo, manifest"),
				qm.Where("id > ?", 1),
				qm.And("name <> ?", "thing"),
				qm.Limit(1),
				qm.GroupBy("id"),
				qm.Offset(1),
			).All(ctx, db)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPopSelectComplex(b *testing.B) {
	dsn := "postgres://BenchmarkPopSelectComplex"

	query := jetQuery()
	query.NumInput = -1
	mimic.NewQueryDSN(dsn, query)

	popdb, err := pop.NewConnection(&pop.ConnectionDetails{Driver: "mimic", Dialect: "postgres", URL: dsn})
	if err != nil {
		panic(err)
	}

	err = popdb.Open()
	if err != nil {
		panic(err)
	}

	b.Run("pop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var store []gorps.Jet
			err = popdb.Select(
				"id, name, color, uuid, identifier, cargo, manifest").
				Where("id > ? AND name <> ?", 1, "thing").
				Limit(1).
				GroupBy("id").
				// Offset does not exist, you need to use a paginator.
				// The Paginator however does another query, which breaks
				// this integration here, so I have removed it.
				//
				//
				// Paginate(1, 1).
				All(&store)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
