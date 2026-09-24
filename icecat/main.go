// Importer Open Icecat → RabbitMQ.
//
// Remplace le scraper HTML LDLC. Consomme une source licenciée (Open Content
// License) au lieu d'extraire un catalogue tiers, et publie exactement le même
// contrat de message dans la queue "products" : l'orchestrateur, les
// consommateurs et le schéma ne changent pas.
//
// Trois modes :
//
//	importer                      import selon ICECAT_CATEGORIES (défaut)
//	importer categories <regex>   liste les catégories Icecat dont le nom correspond
//	importer features <icecat_id> affiche les features d'un produit, pour remplir mapping.json
package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ─── Configuration ───────────────────────────────────────────────────────────

type config struct {
	User        string
	Password    string
	Lang        string
	Categories  map[int]string // Catid → product_type ASIT
	Index       string         // "daily" | "full"
	MaxProducts int
	MaxPerType  int    // plafond par type de produit (0 = aucun)
	MinUpdated  string // YYYYMMDD : ignore les fiches plus anciennes
	Delay       time.Duration
	MappingPath string
	RabbitURL   string
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig(needRabbit bool) config {
	cfg := config{
		User:        os.Getenv("ICECAT_USER"),
		Password:    os.Getenv("ICECAT_PASSWORD"),
		Lang:        strings.ToLower(envOr("ICECAT_LANG", "fr")),
		Index:       envOr("ICECAT_INDEX", "daily"),
		MappingPath: envOr("ICECAT_MAPPING", "/mapping.json"),
		RabbitURL:   os.Getenv("RABBITMQ_URL"),
		Categories:  map[int]string{},
	}
	if cfg.User == "" {
		log.Fatal("ICECAT_USER manquant")
	}
	if needRabbit && cfg.RabbitURL == "" {
		log.Fatal("RABBITMQ_URL manquant")
	}
	cfg.MaxProducts, _ = strconv.Atoi(envOr("ICECAT_MAX_PRODUCTS", "0"))
	cfg.MaxPerType, _ = strconv.Atoi(envOr("ICECAT_MAX_PER_TYPE", "0"))
	cfg.MinUpdated = envOr("ICECAT_MIN_UPDATED", "")
	delayMs, _ := strconv.Atoi(envOr("ICECAT_DELAY_MS", "300"))
	cfg.Delay = time.Duration(delayMs) * time.Millisecond

	// ICECAT_CATEGORIES="server=151,switch=152,router=153,access_point=154"
	for _, pair := range strings.Split(os.Getenv("ICECAT_CATEGORIES"), ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			log.Fatalf("ICECAT_CATEGORIES : entrée invalide %q (attendu type=id)", pair)
		}
		id, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			log.Fatalf("ICECAT_CATEGORIES : id invalide dans %q", pair)
		}
		cfg.Categories[id] = strings.TrimSpace(kv[0])
	}
	return cfg
}

// ─── HTTP ────────────────────────────────────────────────────────────────────

var httpClient = &http.Client{Timeout: 60 * time.Second}

const userAgent = "ASIT-icecat-importer/1.0 (+https://github.com/Architecture-Studio-IT/ASIT)"

// getData télécharge un fichier du serveur data.icecat.biz (authentification Basic).
func getData(cfg config, path string) (io.ReadCloser, error) {
	req, err := http.NewRequest(http.MethodGet, "https://data.icecat.biz/"+path, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(cfg.User, cfg.Password)
	req.Header.Set("User-Agent", userAgent)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s : HTTP %d", path, resp.StatusCode)
	}
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		return struct {
			io.Reader
			io.Closer
		}{gz, resp.Body}, nil
	}
	return resp.Body, nil
}

// ─── Index (files.index.xml / daily.index.xml) ───────────────────────────────

type indexEntry struct {
	ProductID int    `xml:"Product_ID,attr"`
	ProdID    string `xml:"Prod_ID,attr"`
	CatID     int    `xml:"Catid,attr"`
	OnMarket  string `xml:"On_Market,attr"`
	ModelName string `xml:"Model_Name,attr"`
	Updated   string `xml:"Updated,attr"`
}

// collectIndex lit l'index en flux (plusieurs centaines de Mo pour l'index
// complet) et renvoie les produits des catégories voulues, les plus récemment
// mis à jour en premier. L'index est trié par identifiant croissant : sans ce
// tri, une limite de N produits ne ramènerait que les fiches les plus anciennes.
func collectIndex(cfg config) ([]indexEntry, error) {
	var entries []indexEntry
	err := walkIndex(cfg, func(e indexEntry) bool {
		if cfg.MinUpdated != "" && e.Updated < cfg.MinUpdated {
			return false
		}
		entries = append(entries, e)
		return false
	})
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Updated > entries[j].Updated })
	return entries, err
}

// walkIndex lit l'index en flux et appelle fn pour chaque produit des catégories voulues.
func walkIndex(cfg config, fn func(indexEntry) (stop bool)) error {
	name := "daily.index.xml.gz"
	if cfg.Index == "full" {
		name = "files.index.xml.gz"
	}
	path := fmt.Sprintf("export/freexml/%s/%s", strings.ToUpper(cfg.Lang), name)
	log.Println("Index :", path)

	body, err := getData(cfg, path)
	if err != nil {
		return err
	}
	defer body.Close()

	dec := xml.NewDecoder(body)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "file" {
			continue
		}
		var e indexEntry
		if err := dec.DecodeElement(&e, &se); err != nil {
			return err
		}
		if _, wanted := cfg.Categories[e.CatID]; !wanted || e.OnMarket == "0" {
			continue
		}
		if fn(e) {
			return nil
		}
	}
}

// ─── API JSON produit ────────────────────────────────────────────────────────

// flexString accepte une chaîne ou un nombre JSON : Icecat n'est pas constant.
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*f = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = flexString(s)
		return nil
	}
	*f = flexString(strings.Trim(string(b), " "))
	return nil
}

// flexStrings accepte une chaîne, un tableau ou null.
type flexStrings []string

func (f *flexStrings) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*f = nil
		return nil
	}
	if b[0] == '[' {
		var arr []flexString
		if err := json.Unmarshal(b, &arr); err != nil {
			return err
		}
		for _, s := range arr {
			*f = append(*f, string(s))
		}
		return nil
	}
	var s flexString
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*f = flexStrings{string(s)}
	return nil
}

type apiResponse struct {
	Msg string `json:"msg"`
	// En cas d'erreur, Icecat renvoie {"StatusCode":3,"Code":404,"Message":"…"} au lieu de msg.
	StatusCode int    `json:"StatusCode"`
	Message    string `json:"Message"`
	Data       struct {
		GeneralInfo struct {
			IcecatID      flexString  `json:"IcecatId"`
			Title         string      `json:"Title"`
			Brand         string      `json:"Brand"`
			ProductCode   string      `json:"ProductCode"`   // documenté…
			BrandPartCode string      `json:"BrandPartCode"` // …mais c'est cette clé que l'API renvoie
			ProductName   string      `json:"ProductName"`
			GTIN          flexStrings `json:"GTIN"`
			Category      struct {
				CategoryID flexString `json:"CategoryID"`
				Name       struct {
					Value string `json:"Value"`
				} `json:"Name"`
			} `json:"Category"`
		} `json:"GeneralInfo"`
		Image struct {
			HighPic    string `json:"HighPic"`
			LowPic     string `json:"LowPic"`
			Pic500x500 string `json:"Pic500x500"`
		} `json:"Image"`
		Gallery []struct {
			Pic500x500 string `json:"Pic500x500"`
			Pic        string `json:"Pic"`
		} `json:"Gallery"`
		Multimedia []struct {
			URL         string `json:"URL"`
			Type        string `json:"Type"`        // leaflet, manual, video…
			ContentType string `json:"ContentType"` // application/pdf
		} `json:"Multimedia"`
		CatalogObjectCloud struct {
			ProductPage struct {
				URL string `json:"URL"` // permalien public de la fiche Icecat
			} `json:"ProductPage"`
		} `json:"CatalogObjectCloud"`
		FeaturesGroups []struct {
			Features []struct {
				Value             flexString `json:"Value"`
				PresentationValue flexString `json:"PresentationValue"`
				Measure           struct {
					Signs struct {
						Sign string `json:"_"`
					} `json:"Signs"`
				} `json:"Measure"`
				Feature struct {
					ID   flexString `json:"ID"`
					Name struct {
						Value string `json:"Value"`
					} `json:"Name"`
				} `json:"Feature"`
			} `json:"Features"`
		} `json:"FeaturesGroups"`
	} `json:"data"`
}

func fetchProduct(cfg config, icecatID int) (*apiResponse, error) {
	q := url.Values{}
	q.Set("lang", cfg.Lang)
	q.Set("shopname", cfg.User)
	q.Set("icecat_id", strconv.Itoa(icecatID))
	q.Set("content", "") // tout le contenu

	req, err := http.NewRequest(http.MethodGet, "https://live.icecat.biz/api?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var out apiResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("HTTP %d, JSON illisible : %w", resp.StatusCode, err)
	}
	if out.Msg != "OK" {
		detail := out.Message
		if detail == "" {
			detail = out.Msg
		}
		return nil, fmt.Errorf("Icecat (code %d, HTTP %d) : %s", out.StatusCode, resp.StatusCode, detail)
	}
	return &out, nil
}

// ─── Mapping features → colonnes ─────────────────────────────────────────────

type fieldRule struct {
	Type  string   `json:"type"`  // int | text | bool | present | concat | ports
	Match []string `json:"match"` // Feature.ID ou Feature.Name
	// concat : toutes les correspondances trouvées sont jointes par un espace (ex. famille + modèle CPU).
	// ports  : construit [{"type":"RJ45","count":n}, …] à partir d'une entrée par type de port.
	Ports []portRule `json:"ports,omitempty"`
	// contains : vrai si la valeur contient needle (insensible à la casse).
	Needle string `json:"needle,omitempty"`
}

type portRule struct {
	Type  string   `json:"type"`
	Match []string `json:"match"`
}

type mapping map[string]map[string]fieldRule // product_type → colonne → règle

func loadMapping(path string) mapping {
	raw, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("mapping : %v", err)
	}
	var byType map[string]json.RawMessage
	if err := json.Unmarshal(raw, &byType); err != nil {
		log.Fatalf("mapping : JSON invalide : %v", err)
	}
	m := mapping{}
	for t, body := range byType {
		if strings.HasPrefix(t, "_") {
			continue // documentation
		}
		var rules map[string]fieldRule
		if err := json.Unmarshal(body, &rules); err != nil {
			log.Fatalf("mapping : section %q invalide : %v", t, err)
		}
		m[t] = rules
	}
	return m
}

// rawFeature est conservée intégralement dans Product.raw_specs.
type rawFeature struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
	Unit  string `json:"unit,omitempty"`
}

var numRe = regexp.MustCompile(`\d+(?:[.,]\d+)?`)

func toInt(s string) (int, bool) {
	m := numRe.FindString(s)
	if m == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(strings.ReplaceAll(m, ",", "."), 64)
	if err != nil {
		return 0, false
	}
	return int(f + 0.5), true
}

func toBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "y", "yes", "oui", "true", "1", "managed", "géré", "gere":
		return true
	}
	return false
}

// findFeature renvoie la première feature dont l'ID ou le nom correspond à l'un des motifs.
func findFeature(feats []rawFeature, matches []string) *rawFeature {
	for _, m := range matches {
		for i := range feats {
			if feats[i].ID == m || strings.EqualFold(feats[i].Name, m) {
				return &feats[i]
			}
		}
	}
	return nil
}

// project applique les règles d'un type aux features et renvoie les colonnes trouvées.
func project(rules map[string]fieldRule, feats []rawFeature) map[string]any {
	out := map[string]any{}
	for col, rule := range rules {
		switch rule.Type {
		case "ports":
			var ports []map[string]any
			for _, pr := range rule.Ports {
				if f := findFeature(feats, pr.Match); f != nil {
					if n, ok := toInt(f.Value); ok && n > 0 {
						ports = append(ports, map[string]any{"type": pr.Type, "count": n})
					}
				}
			}
			if len(ports) > 0 {
				out[col] = ports
			}
			continue
		case "concat":
			var parts []string
			for _, m := range rule.Match {
				if f := findFeature(feats, []string{m}); f != nil && f.Value != "" {
					parts = append(parts, f.Value)
				}
			}
			if len(parts) > 0 {
				out[col] = strings.Join(parts, " ")
			}
			continue
		}

		hit := findFeature(feats, rule.Match)
		if hit == nil {
			if rule.Type == "present" {
				out[col] = false
			}
			continue
		}
		switch rule.Type {
		case "int":
			if v, ok := toInt(hit.Value); ok {
				out[col] = v
			}
		case "bool":
			out[col] = toBool(hit.Value)
		case "present":
			out[col] = !toBoolNegative(hit.Value)
		case "contains":
			out[col] = strings.Contains(strings.ToLower(hit.Value), strings.ToLower(rule.Needle))
		default:
			v := hit.Value
			if hit.Unit != "" && !strings.Contains(v, hit.Unit) {
				v = v + " " + hit.Unit
			}
			out[col] = v
		}
	}
	return out
}

// toBoolNegative reconnaît une valeur explicitement négative ("Non", "N", "No", "0").
func toBoolNegative(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "n", "no", "non", "false", "0", "":
		return true
	}
	return false
}

// isPublicGTIN accepte EAN-8/13, UPC-A et GTIN-14 à clé valide, hors codes internes.
func isPublicGTIN(s string) bool {
	s = strings.TrimSpace(s)
	n := len(s)
	if n != 8 && n != 12 && n != 13 && n != 14 {
		return false
	}
	allZero := true
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
		if c != '0' {
			allZero = false
		}
	}
	if allZero {
		return false
	}
	switch {
	case n == 13 && s[0] == '2',
		n == 12 && (s[0] == '2' || s[0] == '4'),
		n == 14 && s[1] == '2':
		return false
	}
	sum := 0
	for i := n - 2; i >= 0; i-- {
		d := int(s[i] - '0')
		if (n-2-i)%2 == 0 {
			d *= 3
		}
		sum += d
	}
	return (10-sum%10)%10 == int(s[n-1]-'0')
}

// ─── Contrat de message (identique au scraper précédent, champs ajoutés) ─────

type product struct {
	ProductType  string       `json:"product_type"`
	Name         string       `json:"name"`
	Model        string       `json:"model,omitempty"` // référence courte constructeur
	SKU          string       `json:"sku"`
	GTIN         string       `json:"gtin,omitempty"`
	IcecatID     int          `json:"icecat_id"`
	Source       string       `json:"source"`
	ImageURL     string       `json:"image_url,omitempty"`
	Warranty     string       `json:"warranty,omitempty"`
	ScrapedURL   string       `json:"scraped_url,omitempty"`
	DatasheetURL string       `json:"datasheet_url,omitempty"`
	RawSpecs     []rawFeature `json:"raw_specs"`

	Server      map[string]any `json:"server,omitempty"`
	Switch      map[string]any `json:"switch,omitempty"`
	Router      map[string]any `json:"router,omitempty"`
	AccessPoint map[string]any `json:"access_point,omitempty"`
}

type message struct {
	Name    string  `json:"name"`    // constructeur
	Website string  `json:"website"` // inconnu via Icecat : chaîne vide
	Product product `json:"product"`
}

func buildMessage(resp *apiResponse, productType string, m mapping) (message, error) {
	g := resp.Data.GeneralInfo
	sku := g.BrandPartCode
	if sku == "" {
		sku = g.ProductCode
	}
	if g.Brand == "" || sku == "" {
		return message{}, errors.New("fiche sans marque ou sans référence")
	}

	var feats []rawFeature
	for _, grp := range resp.Data.FeaturesGroups {
		for _, f := range grp.Features {
			val := string(f.PresentationValue)
			if val == "" {
				val = string(f.Value)
			}
			feats = append(feats, rawFeature{
				ID:    string(f.Feature.ID),
				Name:  f.Feature.Name.Value,
				Value: val,
				Unit:  f.Measure.Signs.Sign,
			})
		}
	}

	icecatID, _ := strconv.Atoi(string(g.IcecatID))
	name := g.Title
	if name == "" {
		name = strings.TrimSpace(g.Brand + " " + g.ProductName)
	}
	// Image : 500 px de préférence, sinon la galerie, sinon la vignette 200 px.
	// L'originale (souvent 4000 px, ~600 Ko) est le dernier recours : trop lourde
	// pour une liste. Icecat recommande de toute façon de ré-héberger les images.
	img := resp.Data.Image.Pic500x500
	if img == "" && len(resp.Data.Gallery) > 0 {
		img = resp.Data.Gallery[0].Pic500x500
	}
	if img == "" {
		img = resp.Data.Image.LowPic
	}
	if img == "" {
		img = resp.Data.Image.HighPic
	}
	p := product{
		ProductType:  productType,
		Name:         name,
		Model:        g.ProductName,
		SKU:          sku,
		ScrapedURL:   resp.Data.CatalogObjectCloud.ProductPage.URL,
		DatasheetURL: datasheetURL(resp),
		IcecatID:     icecatID,
		Source:       "icecat",
		ImageURL:     img,
		RawSpecs:     feats,
	}
	// Premier GTIN public : les codes à circulation restreinte (préfixe 2xx)
	// sont internes au distributeur et ne servent à rien pour identifier le produit.
	for _, code := range g.GTIN {
		if isPublicGTIN(code) {
			p.GTIN = code
			break
		}
	}

	// Les types sans table dédiée (écran, clavier, onduleur…) n'ont que raw_specs :
	// l'interface en affiche les premières caractéristiques.
	cols := project(m[productType], feats)
	switch productType {
	case "server":
		p.Server = cols
	case "switch":
		p.Switch = cols
	case "router":
		p.Router = cols
	case "access_point":
		p.AccessPoint = cols
	}

	return message{Name: g.Brand, Website: "", Product: p}, nil
}

// datasheetURL choisit le PDF de la fiche : le manuel constructeur s'il est
// référencé, sinon la brochure générée par Icecat (toutes deux diffusées sous
// Open Content License).
func datasheetURL(resp *apiResponse) string {
	fallback := ""
	for _, m := range resp.Data.Multimedia {
		if !strings.Contains(strings.ToLower(m.ContentType), "pdf") || m.URL == "" {
			continue
		}
		if strings.EqualFold(m.Type, "manual") {
			return m.URL
		}
		if fallback == "" {
			fallback = m.URL
		}
	}
	return fallback
}

// ─── RabbitMQ ────────────────────────────────────────────────────────────────

const rabbitQueue = "products"

type publisher struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func newPublisher(dsn string) (*publisher, error) {
	conn, err := amqp.Dial(dsn)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &publisher{conn: conn, ch: ch}, nil
}

func (p *publisher) publish(msg message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.ch.PublishWithContext(ctx, "", rabbitQueue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (p *publisher) close() {
	p.ch.Close()
	p.conn.Close()
}

// ─── Sous-commandes ──────────────────────────────────────────────────────────

// cmdCategories liste les catégories Icecat dont un nom correspond au motif.
func cmdCategories(cfg config, pattern string) {
	re, err := regexp.Compile("(?i)" + pattern)
	if err != nil {
		log.Fatalf("motif invalide : %v", err)
	}
	body, err := getData(cfg, "export/freexml/refs/CategoriesList.xml.gz")
	if err != nil {
		log.Fatal(err)
	}
	defer body.Close()

	type name struct {
		Value  string `xml:"Value,attr"`
		LangID string `xml:"langid,attr"`
	}
	type category struct {
		ID    string `xml:"ID,attr"`
		Names []name `xml:"Name"`
	}

	dec := xml.NewDecoder(body)
	found := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "Category" {
			continue
		}
		var c category
		if err := dec.DecodeElement(&c, &se); err != nil {
			log.Fatal(err)
		}
		var en, fr string
		match := false
		for _, n := range c.Names {
			if re.MatchString(n.Value) {
				match = true
			}
			switch n.LangID {
			case "1":
				en = n.Value
			case "3":
				fr = n.Value
			}
		}
		if match {
			found++
			fmt.Printf("%-8s %-45s %s\n", c.ID, en, fr)
		}
	}
	fmt.Fprintf(os.Stderr, "%d catégorie(s). Reporter les IDs dans ICECAT_CATEGORIES (type=id).\n", found)
}

// cmdFeatures affiche les features d'un produit pour compléter mapping.json.
func cmdFeatures(cfg config, id int) {
	resp, err := fetchProduct(cfg, id)
	if err != nil {
		log.Fatal(err)
	}
	g := resp.Data.GeneralInfo
	fmt.Printf("%s — %s %s (catégorie %s : %s)\n\n", g.Title, g.Brand, g.ProductCode, g.Category.CategoryID, g.Category.Name.Value)
	fmt.Printf("%-10s %-45s %s\n", "Feature.ID", "Feature.Name", "Value [unit]")
	for _, grp := range resp.Data.FeaturesGroups {
		for _, f := range grp.Features {
			val := string(f.PresentationValue)
			if val == "" {
				val = string(f.Value)
			}
			unit := f.Measure.Signs.Sign
			if unit != "" {
				unit = " [" + unit + "]"
			}
			fmt.Printf("%-10s %-45s %s%s\n", f.Feature.ID, f.Feature.Name.Value, val, unit)
		}
	}
}

// cmdPreview construit le message d'un produit avec le mapping, sans le publier.
// Le type est déduit de ICECAT_CATEGORIES, ou forcé en second argument.
func cmdPreview(cfg config, id int, forcedType string) {
	m := loadMapping(cfg.MappingPath)
	resp, err := fetchProduct(cfg, id)
	if err != nil {
		log.Fatal(err)
	}
	productType := forcedType
	if productType == "" {
		catID, _ := strconv.Atoi(string(resp.Data.GeneralInfo.Category.CategoryID))
		productType = cfg.Categories[catID]
	}
	if productType == "" {
		log.Fatalf("catégorie %s absente de ICECAT_CATEGORIES ; passer le type en second argument", resp.Data.GeneralInfo.Category.CategoryID)
	}
	msg, err := buildMessage(resp, productType, m)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "%d features reçues, conservées dans raw_specs (masquées ici)\n", len(msg.Product.RawSpecs))
	msg.Product.RawSpecs = nil
	out, _ := json.MarshalIndent(msg, "", "  ")
	fmt.Println(string(out))
}

// ─── Import ──────────────────────────────────────────────────────────────────

func runImport(cfg config) {
	if len(cfg.Categories) == 0 {
		log.Fatal("ICECAT_CATEGORIES vide : rien à importer (voir la sous-commande categories)")
	}
	m := loadMapping(cfg.MappingPath)
	warned := map[string]bool{}
	for _, t := range cfg.Categories {
		if _, ok := m[t]; !ok && !warned[t] {
			warned[t] = true
			log.Printf("type %q sans section dans mapping.json : seules les specs brutes seront conservées", t)
		}
	}

	pub, err := newPublisher(cfg.RabbitURL)
	if err != nil {
		log.Fatalf("RabbitMQ : %v", err)
	}
	defer pub.close()
	log.Println("Connecté à RabbitMQ, queue :", rabbitQueue)

	start := time.Now()
	entries, err := collectIndex(cfg)
	if err != nil {
		log.Printf("index : %v", err)
	}
	total := len(entries)
	if cfg.MaxPerType > 0 {
		perType := map[string]int{}
		kept := entries[:0]
		for _, e := range entries {
			t := cfg.Categories[e.CatID]
			if perType[t] >= cfg.MaxPerType {
				continue
			}
			perType[t]++
			kept = append(kept, e)
		}
		entries = kept
	}
	if cfg.MaxProducts > 0 && len(entries) > cfg.MaxProducts {
		entries = entries[:cfg.MaxProducts]
	}
	log.Printf("Index lu en %s : %d fiches candidates, %d retenues", time.Since(start).Round(time.Second), total, len(entries))

	published, failed := 0, 0
	for _, e := range entries {
		time.Sleep(cfg.Delay)

		resp, err := fetchProduct(cfg, e.ProductID)
		if err != nil {
			failed++
			log.Printf("✗ %d (%s) : %v", e.ProductID, e.ProdID, err)
			continue
		}
		msg, err := buildMessage(resp, cfg.Categories[e.CatID], m)
		if err != nil {
			failed++
			log.Printf("✗ %d (%s) : %v", e.ProductID, e.ProdID, err)
			continue
		}
		if err := pub.publish(msg); err != nil {
			failed++
			log.Printf("✗ publish %d : %v", e.ProductID, err)
			continue
		}
		published++
		log.Printf("✓ %s — %s (%s)", msg.Product.ProductType, msg.Product.Name, msg.Product.SKU)
	}
	log.Printf("Terminé en %s : %d publiés, %d en échec", time.Since(start).Round(time.Second), published, failed)
}

func main() {
	log.SetFlags(log.Ltime)
	args := os.Args[1:]
	switch {
	case len(args) >= 2 && args[0] == "categories":
		cmdCategories(loadConfig(false), args[1])
	case len(args) >= 2 && args[0] == "preview":
		id, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatal("preview : icecat_id numérique attendu")
		}
		forced := ""
		if len(args) >= 3 {
			forced = args[2]
		}
		cmdPreview(loadConfig(false), id, forced)
	case len(args) >= 2 && args[0] == "features":
		id, err := strconv.Atoi(args[1])
		if err != nil {
			log.Fatal("features : icecat_id numérique attendu")
		}
		cmdFeatures(loadConfig(false), id)
	case len(args) == 0:
		runImport(loadConfig(true))
	default:
		fmt.Fprintln(os.Stderr, "usage : importer | importer categories <regex> | importer features <icecat_id> | importer preview <icecat_id> [type]")
		os.Exit(2)
	}
}
