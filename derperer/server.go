package derperer

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/monitor"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/gofiber/swagger"
	_ "github.com/koyangyang/derperer/docs"
	"github.com/koyangyang/derperer/persistent"
	"github.com/sourcegraph/conc"
	"go.uber.org/zap"
)

type Derperer struct {
	config     DerpererConfig
	app        *fiber.App
	derpMap    *Map
	persistent *persistent.Persistent
}

type DerpererConfig struct {
	Address        string        `json:"address"`
	AdminToken     string        `json:"adminToken"`
	FetchInterval  time.Duration `json:"fetchInterval"`
	DERPMapPolicy  DERPMapPolicy `json:"derpMapPolicy"`
	DataPath       string        `json:"dataPath"`
	Account        string        `json:"account"`
	ApiKey         string        `json:"apiKey"`
	UpdateInterval time.Duration `json:"updateInterval"`
	DeleteInterval time.Duration `json:"deleteInterval"`
}

type DERPCandidate struct {
	IP             string `json:"ip"`
	Port           string `json:"port"`
	Host           string `json:"host"`
	Protocol       string `json:"protocol"`
	Country        string `json:"country"`
	Region         string `json:"region"`
	City           string `json:"city"`
	ASOrganization string `json:"as_organization"`
}

// LocalDataRecord represents one line in DataPath/result.json.
type LocalDataRecord struct {
	BaseProtocol   string `json:"base_protocol"`
	ASOrganization string `json:"as_organization"`
	City           string `json:"city"`
	Country        string `json:"country"`
	Host           string `json:"host"`
	IP             string `json:"ip"`
	Port           string `json:"port"`
	Region         string `json:"region"`
}

func NewDerperer(config DerpererConfig) (*Derperer, error) {
	app := fiber.New()

	p, err := persistent.NewPersistent(config.DataPath)
	if err != nil {
		return nil, err
	}

	derperer := &Derperer{
		config:     config,
		app:        app,
		derpMap:    NewMap(&config.DERPMapPolicy),
		persistent: p,
	}

	if err := derperer.persistent.Load("derp_map", derperer.derpMap); err != nil {
		zap.L().Warn("failed to load derp_map from persistent storage, will rebuild from result.json", zap.Error(err))
		derperer.LoadSourceData()
	}

	app.Get("/swagger/*", swagger.HandlerDefault)

	app.Get("/", derperer.index)
	app.Get("/derp.json", derperer.getDerp)
	app.Get("/derp_sort.json", derperer.sortDerp)
	app.Get("/update", derperer.updateTailscale)

	if config.AdminToken != "" {
		adminAPI := app.Group("/admin", basicauth.New(basicauth.Config{
			Users: map[string]string{
				"admin": config.AdminToken,
			},
		}))

		adminAPI.Get("/", derperer.adminIndex)
		adminAPI.Get("/monitor", monitor.New())
		adminAPI.Use(pprof.New(pprof.Config{
			Prefix: "/admin",
		}))
		adminAPI.Get("/config", derperer.getConfig)
		adminAPI.Post("/config", derperer.setConfig)
	}

	return derperer, nil
}

// ToDERPCandidate normalizes a raw result.json line into a candidate node.
func (r *LocalDataRecord) ToDERPCandidate() DERPCandidate {
	protocol := strings.ToLower(strings.TrimSpace(r.BaseProtocol))
	if protocol == "" {
		protocol = "http"
	}
	if strings.HasPrefix(r.Host, "https://") {
		protocol = "https"
	} else if strings.HasPrefix(r.Host, "http://") {
		protocol = "http"
	}

	host := r.Host
	if !strings.Contains(host, "://") {
		host = protocol + "://" + host
	}

	return DERPCandidate{
		IP:             r.IP,
		Port:           r.Port,
		Host:           host,
		Protocol:       protocol,
		Country:        r.Country,
		Region:         r.Region,
		City:           r.City,
		ASOrganization: r.ASOrganization,
	}
}

func (d *Derperer) LoadDataFromFile() error {
	logger := zap.L()
	filePath := filepath.Join(d.config.DataPath, "result.json")
	logger.Info("loading data from file", zap.String("path", filePath))

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open data file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var record LocalDataRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			logger.Error("failed to parse json line", zap.Error(err), zap.String("line", scanner.Text()))
			continue
		}

		candidate := record.ToDERPCandidate()
		if err := d.derpMap.AddCandidate(candidate); err != nil {
			logger.Debug("failed to add result", zap.Error(err), zap.String("host", record.Host))
			continue
		}
		count++

		// Persist progress periodically when importing large files.
		if count%10 == 0 {
			if err := d.persistent.Save("derp_map", d.derpMap); err != nil {
				logger.Error("failed to save derp_map", zap.Error(err))
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	if err := d.persistent.Save("derp_map", d.derpMap); err != nil {
		logger.Error("failed to save derp_map", zap.Error(err))
	}

	logger.Info("finished loading data from file", zap.Int("count", count))
	return nil
}

func (d *Derperer) ResetSourceData() {
	logger := zap.L()
	logger.Info("resetting cached derp map")
	if err := d.persistent.Delete("derp_map"); err != nil {
		logger.Error("failed to delete derp_map", zap.Error(err))
	}
	d.derpMap = NewMap(&d.config.DERPMapPolicy)
	d.LoadSourceData()
}

func (d *Derperer) LoadSourceData() {
	logger := zap.L()
	logger.Info("loading data from local file")
	if err := d.LoadDataFromFile(); err != nil {
		logger.Error("failed to load data from file", zap.Error(err))
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
			d.LoadSourceData()

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

			<-time.After(d.config.DeleteInterval - time.Since(lastFetch))
			d.ResetSourceData()

			if err := d.persistent.Save("last_delete", time.Now()); err != nil {
				zap.L().Error("failed to save last_delete", zap.Error(err))
			}
		}
	})

	wg.Go(func() {
		if err := d.app.Listen(d.config.Address); err != nil {
			zap.L().Fatal("failed to listen", zap.Error(err))
		}
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
// @Param all query boolean false "return all regions without filtering"
// @Param status query string false "alive|error|unknown" Enums(alive, error, unknown)
// @Param latency-limit query string false "latency limit, e.g. 500ms"
// @Param bandwidth-limit query string false "bandwidth limit, e.g. 2Mbps"
// @Produce json
// @Success 200 {object} derperer.DERPMap
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

// @Summary Get Top DERP Regions
// @Produce json
// @Success 200 {object} derperer.DERPResult
// @Router /derp_sort.json [get]
func (d *Derperer) sortDerp(c *fiber.Ctx) error {
	m, err := d.derpMap.SortTopKDERPMap(20)
	if err != nil {
		return err
	}
	return c.JSON(m)
}

// @Summary Update Tailscale ACL
// @Produce json
// @Success 200 {string} string
// @Router /update [get]
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
// @Success 200 {object} derperer.DerpererConfig
// @Router /admin/config [get]
func (d *Derperer) getConfig(c *fiber.Ctx) error {
	return c.JSON(d.config)
}

// @Summary Change Server Config
// @Accept json
// @Param config body derperer.DerpererConfig true "config"
// @Produce json
// @Security BasicAuth
// @Success 200 {object} derperer.DerpererConfig
// @Router /admin/config [post]
func (d *Derperer) setConfig(c *fiber.Ctx) error {
	if err := c.BodyParser(&d.config); err != nil {
		return err
	}
	return c.JSON(d.config)
}
