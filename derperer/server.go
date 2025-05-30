package derperer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/monitor"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/gofiber/swagger"
	_ "github.com/koyangyang/derperer/docs"
	"github.com/koyangyang/derperer/fofa"
	"github.com/koyangyang/derperer/persistent"
	"github.com/sourcegraph/conc"
	"go.uber.org/zap"
)

const FINGERPRINT = `"<h1>DERP</h1>"`

type Derperer struct {
	config     DerpererConfig
	app        *fiber.App
	ctx        context.Context
	derpMap    *Map
	persistent *persistent.Persistent
}

type DerpererConfig struct {
	Address        string
	AdminToken     string
	FetchInterval  time.Duration
	FetchBatch     int
	FofaClient     fofa.Fofa
	DERPMapPolicy  DERPMapPolicy
	DataPath       string
	Account        string
	ApiKey         string
	UpdateInterval time.Duration
	DeleteInterval time.Duration
}

func NewDerperer(config DerpererConfig) (*Derperer, error) {
	app := fiber.New()
	ctx := context.Background()

	p, err := persistent.NewPersistent(config.DataPath)
	if err != nil {
		return nil, err
	}

	if config.FofaClient.Email == "" || config.FofaClient.Key == "" {
		return nil, fmt.Errorf("fofa email and key must be set")
	}

	derperer := &Derperer{
		config:     config,
		app:        app,
		ctx:        ctx,
		derpMap:    NewMap(&config.DERPMapPolicy),
		persistent: p,
	}

	if err := derperer.persistent.Load("derp_map", derperer.derpMap); err != nil {
		zap.L().Error("failed to load derp_map", zap.Error(err))
	}

	app.Get("/swagger/*", swagger.HandlerDefault)

	app.Get("/", derperer.index)

	app.Get("/derp.json", derperer.getDerp)
	app.Get("/derp_sort.json", derperer.sortDerp)
	app.Get("/update", derperer.updateTailscale)

	if config.AdminToken != "" {
		adminApi := app.Group("/admin", basicauth.New(basicauth.Config{
			Users: map[string]string{
				"admin": config.AdminToken,
			},
		}))

		adminApi.Get("/", derperer.adminIndex)

		adminApi.Get("/monitor", monitor.New())
		adminApi.Use(pprof.New(pprof.Config{
			Prefix: "/admin",
		}))
		adminApi.Get("/config", derperer.getConfig)
		adminApi.Post("/config", derperer.setConfig)
	}

	return derperer, nil
}

func (d *Derperer) RemoveFofaData() {
	logger := zap.L()
	logger.Info("removing fofa data")
	if err := d.persistent.Delete("derp_map"); err != nil {
		logger.Error("failed to delete derp_map", zap.Error(err))
	}
	d.derpMap = NewMap(&d.config.DERPMapPolicy)
	d.FetchFofaData()
}

func (d *Derperer) FetchFofaData() {
	logger := zap.L()
	logger.Info("fetching fofa")
	res, finish, err := d.config.FofaClient.Query(FINGERPRINT, d.config.FetchBatch, -1)
	if err != nil {
		logger.Error("failed to query fofa", zap.Error(err))
	}
	for {
		select {
		case r := <-res:
			d.derpMap.AddFofaResult(r)

			if err := d.persistent.Save("derp_map", d.derpMap); err != nil {
				logger.Error("failed to save derp_map", zap.Error(err))
			}
		case <-finish:
			logger.Info("fofa query finished")
			return
		}
	}
}

func (d *Derperer) Start() {
	wg := conc.WaitGroup{}

	wg.Go(d.derpMap.Recheck)

	wg.Go(func() {
		for {
			var lastFetch time.Time
			if err := d.persistent.Load("last_fetch", &lastFetch); err != nil {
				zap.L().Error("failed to load last_fetch", zap.Error(err))
			}

			<-time.After(d.config.FetchInterval - time.Since(lastFetch))
			d.FetchFofaData()

			if err := d.persistent.Save("last_fetch", time.Now()); err != nil {
				zap.L().Error("failed to save last_fetch", zap.Error(err))
			}
		}
	})

	wg.Go(func() {
		for {
			var lastFetch time.Time
			if err := d.persistent.Load("last_fetch_tailscale", &lastFetch); err != nil {
				zap.L().Error("failed to load last_fetch_tailscale", zap.Error(err))
			}

			<-time.After(d.config.UpdateInterval - time.Since(lastFetch))
			d.autoupdateTailscale()

			if err := d.persistent.Save("last_fetch_tailscale", time.Now()); err != nil {
				zap.L().Error("failed to save last_fetch_tailscale", zap.Error(err))
			}
		}
	})

	wg.Go(func() {
		for {
			var lastFetch time.Time
			if err := d.persistent.Load("last_delete", &lastFetch); err != nil {
				zap.L().Error("failed to load last_delete", zap.Error(err))
			}

			<-time.After(d.config.FetchInterval - time.Since(lastFetch))
			d.RemoveFofaData()

			if err := d.persistent.Save("last_delete", time.Now()); err != nil {
				zap.L().Error("failed to save last_delete", zap.Error(err))
			}
		}
	})

	wg.Go(func() {
		d.app.Listen(d.config.Address)
	})

	wg.Wait()
}

// @Summary Index
// @Produce html
// @Router / [get]
func (d *Derperer) index(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/html")
	return c.SendString(strings.TrimSpace(`
<a href="/derp.json">derp.json</a><br/>
<a href="/swagger/index.html">swagger</a><br/>
<a href="/admin">admin</a><br/>
		`))
}

// @Summary Get DERP Map
// @Param status query string false "alive|error|all" Enums(alive, error, all)
// @Param latency-limit query string false "latency limit, e.g. 500ms"
// @Param bandwidth-limit query string string "bandwidth limit, e.g. 2Mbps"
// @Produce json
// @Router /derp.json [get]
func (d *Derperer) getDerp(c *fiber.Ctx) error {
	var filter DERPMapFilter
	if err := c.QueryParser(&filter); err != nil {
		return err
	}
	m, err := d.derpMap.FilterDERPMap(filter)
	if err != nil {
		return err
	}
	return c.JSON(m)
}

func (d *Derperer) sortDerp(c *fiber.Ctx) error {
	m, err := d.derpMap.SortTopKDERPMap(20)
	if err != nil {
		return err
	}
	return c.JSON(m)
}

func (d *Derperer) updateTailscale(c *fiber.Ctx) error {
	m, err := d.derpMap.SortTopKDERPMap(20)
	if err != nil {
		return err
	}
	t := UpdateACL(m.Regions, d.config.Account, d.config.ApiKey)
	return c.JSON(t)
}

func (d *Derperer) autoupdateTailscale() {
	m, err := d.derpMap.SortTopKDERPMap(20)
	if err != nil {
		return
	}
	t := UpdateACL(m.Regions, d.config.Account, d.config.ApiKey)
	fmt.Println(t)
}

// @securityDefinitions.basic BasicAuth

// @Summary Admin Index
// @Produce html
// @Security BasicAuth
// @Router /admin [get]
func (d *Derperer) adminIndex(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/html")
	return c.SendString(`
	<a href="/admin/monitor">monitor</a><br/>
	<a href="/admin/debug/pprof">pprof</a><br/>
	<a href="/admin/config">config</a> or <code>POST</code> to change config <br/>
	`)
}

// @Summary Get Server Config
// @Produce json
// @Security BasicAuth
// @Router /admin/config [get]
func (d *Derperer) getConfig(c *fiber.Ctx) error {
	return c.JSON(d.config)
}

// @Summary Change Server Config
// @Accept json
// @Param config body derperer.DerpererConfig true "config"
// @Produce json
// @Security BasicAuth
// @Router /admin/config [post]
func (d *Derperer) setConfig(c *fiber.Ctx) error {
	if err := c.BodyParser(&d.config); err != nil {
		return err
	}
	return c.JSON(d.config)
}
