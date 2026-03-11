package main

import (
	"context"
	"encoding/json"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
	"os"

	"github.com/gocolly/colly/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ─── JSON sub-types (champs Json? dans Prisma) ────────────────────────────────

type StorageBay struct {
	Type  string `json:"type"`  // "3.5", "2.5", "M.2", "NVMe", etc.
	Count int    `json:"count"`
}

type LanPort struct {
	Type       string `json:"type"`        // "RJ45", "SFP", "SFP+"
	Count      int    `json:"count"`
	SpeedsMbps []int  `json:"speeds_mbps"` // toutes les vitesses supportées : [10, 100, 1000]
	MaxSpeedMbps int  `json:"max_speed_mbps"` // vitesse max pour tri/filtres rapides
}

// ─── Structs : Manufacturer → Product → ProductServer ────────────────────────

type ProductServer struct {
	// CPU
	CPUModel      string `json:"cpu_model,omitempty"`
	CPUCores      int    `json:"cpu_cores,omitempty"`
	CPUFrequency  string `json:"cpu_frequency,omitempty"`
	CPUSocket     string `json:"cpu_socket,omitempty"`

	// RAM
	RamGB         int    `json:"ram_gb,omitempty"`
	RamType       string `json:"ram_type,omitempty"`
	RamMaxGB      int    `json:"ram_max_gb,omitempty"`
	RamSlotsTotal int    `json:"ram_slots_total,omitempty"`
	RamSlotsAvail int    `json:"ram_slots_available,omitempty"`

	// Stockage — storage_bays est un []StorageBay → sérialisé en Json
	StorageSlotsTotal int          `json:"storage_slots_total,omitempty"`
	StorageBays       []StorageBay `json:"storage_bays,omitempty"`
	StorageCapacity   string       `json:"storage_capacity,omitempty"`
	StorageConnector  string       `json:"storage_connector,omitempty"`

	// Boîtier
	RackUnits    int    `json:"rack_units,omitempty"`
	CaseFormat   string `json:"case_format,omitempty"`
	ServerFormat string `json:"server_format,omitempty"`

	// Réseau — lan_ports agrège ethernet + sfp + vitesse → Json
	LanPorts []LanPort `json:"lan_ports,omitempty"`

	// Alimentation
	PowerWatts   int  `json:"power_watts,omitempty"`
	RedundantPSU bool `json:"redundant_psu"`

	// Connectique — Json? dans Prisma, []string compatible
	FrontConnectors []string `json:"front_connectors,omitempty"`
	RearConnectors  []string `json:"rear_connectors,omitempty"`

	// Divers
	Barebone   bool `json:"barebone"`
	OSIncluded bool `json:"os_included"`

	// Dimensions
	HeightMM int `json:"height_mm,omitempty"`
	WidthMM  int `json:"width_mm,omitempty"`
	DepthMM  int `json:"depth_mm,omitempty"`
}

// Product reflète exactement le modèle Prisma Product
type Product struct {
	ProductType string `json:"product_type"`
	Name        string `json:"name"`
	SKU         string `json:"sku,omitempty"`
	ImageURL    string `json:"image_url,omitempty"`
	Warranty    string `json:"warranty,omitempty"`   // ← sur Product, pas Server
	ScrapedURL  string `json:"scraped_url,omitempty"`

	Server *ProductServer `json:"server,omitempty"`
}

// Manufacturer embarque un seul Product → 1 message RabbitMQ = 1 produit
type Manufacturer struct {
	Name    string   `json:"name"`
	Website string   `json:"website"`
	Product *Product `json:"product"`
}

// ─── RabbitMQ ─────────────────────────────────────────────────────────────────

type Publisher struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

func NewPublisher(dsn, queue string) (*Publisher, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Publisher{conn: conn, ch: ch, queue: queue}, nil
}

func (p *Publisher) Publish(msg Manufacturer) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.ch.PublishWithContext(ctx, "", p.queue, false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

func (p *Publisher) Close() {
	p.ch.Close()
	p.conn.Close()
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func extractInt(s string) int {
	re := regexp.MustCompile(`\d+`)
	match := re.FindString(s)
	if match == "" {
		return 0
	}
	i, _ := strconv.Atoi(match)
	return i
}

func extractBool(s string) bool {
	s = strings.ToLower(s)
	return strings.Contains(s, "oui") || strings.Contains(s, "yes")
}

func cleanText(s string) string {
	re := regexp.MustCompile(`\s+`)
	return strings.TrimSpace(re.ReplaceAllString(s, " "))
}


func extractCellText(cell *colly.HTMLElement) string {
	raw := cleanText(cell.Text)
	linkText := cleanText(cell.ChildText("a"))
	if linkText != "" {
		re := regexp.MustCompile(`^(\d+\s*[Xx]\s*)`)
		prefix := re.FindString(raw)
		return cleanText(prefix + linkText)
	}
	return raw
}

// parseStorageBay convertit "2 X Baies 3.5\"" ou "1 baie M.2" → StorageBay
// Exemples de valeurs LDLC : "1 X Baie 3.5\"", "2 X Baies M.2", "4 baies NVMe"
func parseStorageBay(val string) StorageBay {
	count := extractInt(val) // premier entier = quantité
	if count == 0 {
		count = 1
	}

	low := strings.ToLower(val)
	var bayType string
	switch {
	case strings.Contains(low, "nvme"):
		bayType = "NVMe"
	case strings.Contains(low, "m.2"):
		bayType = "M.2"
	case strings.Contains(low, "2.5"):
		bayType = "2.5"
	case strings.Contains(low, "3.5"):
		bayType = "3.5"
	case strings.Contains(low, "u.2"):
		bayType = "U.2"
	default:
		// Fallback : on garde la valeur brute nettoyée
		re := regexp.MustCompile(`(?i)(baie[s]?|slot[s]?|\d+\s*[Xx]\s*)`)
		bayType = strings.TrimSpace(re.ReplaceAllString(val, ""))
		if bayType == "" {
			bayType = val
		}
	}
	return StorageBay{Type: bayType, Count: count}
}

// parseAllLanSpeeds extrait TOUTES les vitesses depuis une norme réseau, en Mbps.
// Exemples :
//   "10/100/1000 Mbps"       → [10, 100, 1000]
//   "10GbE"                  → [10000]
//   "100 Mbps / 1 Gbps"      → [100, 1000]
//   "2.5 GbE"                → [2500]
//   "10/25/40/100 GbE"       → [10000, 25000, 40000, 100000]
func parseAllLanSpeeds(s string) []int {
	low := strings.ToLower(s)

	// Détecte si la chaîne exprime des Gbps (présence de "gbps", "gbe", "gb/s", "g ")
	isGbps := regexp.MustCompile(`g(?:bps|be|b/s|\s|$)`).MatchString(low)

	// Extrait tous les nombres (entiers ou décimaux comme "2.5")
	reNum := regexp.MustCompile(`\d+(?:\.\d+)?`)
	rawMatches := reNum.FindAllString(s, -1)

	seen := map[int]bool{}
	result := []int{}

	for _, m := range rawMatches {
		f, err := strconv.ParseFloat(m, 64)
		if err != nil {
			continue
		}
		var mbps int
		if isGbps {
			mbps = int(f * 1000)
		} else {
			mbps = int(f)
		}
		// Filtre les valeurs absurdes (ex: année "2024" dans le texte)
		if mbps == 0 || mbps > 800000 {
			continue
		}
		if !seen[mbps] {
			seen[mbps] = true
			result = append(result, mbps)
		}
	}
	return result
}

// ─── Mapping clé/valeur → ProductServer ──────────────────────────────────────

// scratchpad de construction réseau, utilisé pendant le parsing d'une fiche
type netScratch struct {
	ethernetCount int
	sfpCount      int
	sfpPlusCount  int
	speedsMbps    []int // toutes les vitesses extraites, ex: [10, 100, 1000]
}

func (n *netScratch) toLanPorts() []LanPort {
	speeds := n.speedsMbps
	if len(speeds) == 0 {
		speeds = []int{}
	}
	maxSpeed := 0
	for _, s := range speeds {
		if s > maxSpeed {
			maxSpeed = s
		}
	}

	ports := []LanPort{}
	if n.ethernetCount > 0 {
		ports = append(ports, LanPort{Type: "RJ45", Count: n.ethernetCount, SpeedsMbps: speeds, MaxSpeedMbps: maxSpeed})
	}
	if n.sfpCount > 0 {
		ports = append(ports, LanPort{Type: "SFP", Count: n.sfpCount, SpeedsMbps: speeds, MaxSpeedMbps: maxSpeed})
	}
	if n.sfpPlusCount > 0 {
		ports = append(ports, LanPort{Type: "SFP+", Count: n.sfpPlusCount, SpeedsMbps: speeds, MaxSpeedMbps: maxSpeed})
	}
	return ports
}

func applyField(srv *ProductServer, p *Product, net *netScratch, key, val string, storageSlotParts *[]StorageBay) {
	switch key {
	// ── Identification ────────────────────────────────────────────────────
	case "modèle":
		p.SKU = val
	case "garantie commerciale":
		p.Warranty = val // ← sur Product, pas Server

	// ── Processeur ────────────────────────────────────────────────────────
	case "processeur", "type de processeur":
		srv.CPUModel = val
	case "fréquence cpu":
		srv.CPUFrequency = val
	case "support du processeur":
		srv.CPUSocket = val
	case "nombre de processeur(s) installé(s)":
		srv.CPUCores = extractInt(val)

	// ── Mémoire ───────────────────────────────────────────────────────────
	case "taille de la mémoire":
		srv.RamGB = extractInt(val)
	case "taille de mémoire max":
		srv.RamMaxGB = extractInt(val)
	case "type de mémoire":
		srv.RamType = val
	case "nombre total de slots mémoire":
		srv.RamSlotsTotal = extractInt(val)
	case "nombre de slots mémoire disponibles":
		srv.RamSlotsAvail = extractInt(val)

	// ── Stockage ──────────────────────────────────────────────────────────
	case "nombre de baies pour disques":
		bay := parseStorageBay(val)
		*storageSlotParts = append(*storageSlotParts, bay)
		srv.StorageSlotsTotal += bay.Count
		srv.StorageBays = *storageSlotParts
	case "capacité":
		srv.StorageCapacity = val
	case "connecteurs disques":
		srv.StorageConnector = val

	// ── Réseau ────────────────────────────────────────────────────────────
	case "ports ethernet", "port ethernet":
		net.ethernetCount = extractInt(val)
	case "ports sfp+", "port sfp+":
		net.sfpPlusCount = extractInt(val)
	case "ports sfp", "port sfp":
		net.sfpCount = extractInt(val)
	case "norme(s) réseau":
		net.speedsMbps = parseAllLanSpeeds(val)

	// ── Boîtier & alimentation ────────────────────────────────────────────
	case "format du boitier":
		srv.RackUnits = extractInt(val)
		srv.CaseFormat = val
	case "format serveur":
		srv.ServerFormat = val
	case "puissance":
		srv.PowerWatts = extractInt(val)
	case "alimentation redondante":
		srv.RedundantPSU = extractBool(val)

	// ── Connectique ───────────────────────────────────────────────────────
	case "connecteurs panneau avant":
		srv.FrontConnectors = append(srv.FrontConnectors, val)
	case "connecteurs panneau arrière":
		srv.RearConnectors = append(srv.RearConnectors, val)

	// ── Dimensions ────────────────────────────────────────────────────────
	case "hauteur":
		srv.HeightMM = extractInt(val)
	case "largeur":
		srv.WidthMM = extractInt(val)
	case "profondeur":
		srv.DepthMM = extractInt(val)

	// ── Logiciel & divers ─────────────────────────────────────────────────
	case "système d'exploitation fourni":
		srv.OSIncluded = extractBool(val)
	case "barebone serveur":
		srv.Barebone = extractBool(val)

	// ── Champs à ignorer explicitement ────────────────────────────────────
	case "chipset", "nombre de cpu supportés",
		"fréquence(s) mémoire",
		"lecteur optique", "lecteur de disquettes",
		"wifi", "port console", "contrôleur réseau intégré",
		"type de garantie",
		"personne responsable", "adresse postale",
		"adresse électronique", "documentation", "garantie légale",
		"marque", "désignation":
		// Pas de colonne Prisma pour ces champs → on drop proprement
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func getRabbitDSN() string {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		log.Fatal("RABBITMQ_URL env variable is not set")
	}
	return url
}

const rabbitQueue = "products"

func main() {
	pub, err := NewPublisher(getRabbitDSN(), rabbitQueue)
	if err != nil {
		log.Fatalf("RabbitMQ connection failed: %v", err)
	}
	defer pub.Close()
	log.Println("Connecté à RabbitMQ, queue :", rabbitQueue)

	mfrName    := "LDLC Pro"
	mfrWebsite := "https://www.ldlc.pro"

	categoryURL := "https://www.ldlc.pro/reseaux/serveur/c5391/"
	log.Println("Scraping catégorie :", categoryURL)

	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)

	productLinks := []string{}
	seen := map[string]bool{}

	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		href := e.Attr("href")
		if strings.Contains(href, "/fiche/") {
			full := e.Request.AbsoluteURL(href)
			if !seen[full] {
				seen[full] = true
				productLinks = append(productLinks, full)
				log.Println("Lien produit :", full)
			}
		}
	})

	c.OnScraped(func(r *colly.Response) {
		log.Println("Fin scraping catégorie. Total :", len(productLinks))
	})

	c.Visit(categoryURL)

	if len(productLinks) == 0 {
		log.Println("Aucun lien produit trouvé.")
		return
	}

	productCollector := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)

	productCollector.OnRequest(func(r *colly.Request) {
		log.Println("Scraping fiche :", r.URL.String())
	})

	productCollector.OnHTML("body", func(e *colly.HTMLElement) {
		srv := &ProductServer{}
		net := &netScratch{}
		p := &Product{
			ProductType: "server",
			ScrapedURL:  e.Request.URL.String(),
			Server:      srv,
		}

		p.Name = cleanText(e.ChildText("h1.title-1"))
		if p.Name == "" {
			p.Name = cleanText(e.ChildText("h1"))
		}

		p.ImageURL = e.ChildAttr(".product-img img", "src")
		if p.ImageURL == "" {
			p.ImageURL = e.ChildAttr(".product-img img", "data-src")
		}

		var currentKey string
		var storageSlotParts []StorageBay

		e.ForEach("#product-parameters tr", func(_ int, row *colly.HTMLElement) {
			if row.Attr("class") == "feature" {
				return
			}

			labelEl := row.DOM.Find("td.label h3")
			if labelEl.Length() > 0 {
				newKey := strings.ToLower(cleanText(labelEl.Text()))
				if newKey != "" && newKey != currentKey {
					currentKey = newKey
					if currentKey != "nombre de baies pour disques" {
						storageSlotParts = nil
					}
				}
			}

			if currentKey == "" {
				return
			}

			row.ForEach("td.checkbox, td.no-checkbox", func(_ int, cell *colly.HTMLElement) {
				val := extractCellText(cell)
				if val == "" {
					return
				}
				log.Printf("   [%s] = [%s]\n", currentKey, val)
				applyField(srv, p, net, currentKey, val, &storageSlotParts)
			})
		})

		// Finalise lan_ports depuis le scratchpad réseau
		srv.LanPorts = net.toLanPorts()

		msg := Manufacturer{
			Name:    mfrName,
			Website: mfrWebsite,
			Product: p,
		}

		if err := pub.Publish(msg); err != nil {
			log.Printf("ERREUR publish RabbitMQ [%s] : %v\n", p.SKU, err)
			return
		}

		log.Printf("✓ Publié dans RabbitMQ : %s (%s)\n", p.Name, p.SKU)
	})

	for _, link := range productLinks {
		productCollector.Visit(link)
	}
}