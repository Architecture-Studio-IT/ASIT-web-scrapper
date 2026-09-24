package main

import (
	"encoding/json"
	"testing"
)

// Extrait représentatif de la réponse JSON documentée par Icecat.
const sample = `{
  "msg": "OK",
  "data": {
    "GeneralInfo": {
      "IcecatId": 12345,
      "Title": "Dell PowerEdge R660 server",
      "Brand": "DELL",
      "BrandPartCode": "R660-1234", "ProductName": "R660",
      "GTIN": ["2000001403587", "5397184657812"],
      "Category": {"CategoryID": 151, "Name": {"Value": "Servers", "Language": "EN"}}
    },
    "Image": {"HighPic": "https://images.icecat.biz/img/hi.jpg", "Pic500x500": "https://images.icecat.biz/img/500.jpg"},
    "FeaturesGroups": [
      {"Features": [
        {"Value": "Intel Xeon Silver 4410Y", "PresentationValue": "Intel Xeon Silver 4410Y", "Measure": {"Signs": {"_": ""}}, "Feature": {"ID": "9016", "Name": {"Value": "Processor model"}}},
        {"Value": "12", "PresentationValue": "12", "Measure": {"Signs": {"_": ""}}, "Feature": {"ID": 5563, "Name": {"Value": "Processor cores"}}},
        {"Value": "32", "PresentationValue": "32 GB", "Measure": {"Signs": {"_": "GB"}}, "Feature": {"ID": "441", "Name": {"Value": "Internal memory"}}},
        {"Value": "Y", "PresentationValue": "Y", "Measure": {"Signs": {"_": ""}}, "Feature": {"ID": "12345", "Name": {"Value": "Redundant power supply (PSU)"}}},
        {"Value": "1U", "PresentationValue": "1U", "Measure": {"Signs": {"_": ""}}, "Feature": {"ID": "999", "Name": {"Value": "Rack capacity"}}}
      ]}
    ]
  }
}`

func TestBuildMessage(t *testing.T) {
	var resp apiResponse
	if err := json.Unmarshal([]byte(sample), &resp); err != nil {
		t.Fatalf("unmarshal : %v", err)
	}
	m := mapping{"server": {
		"cpu_model":     {Type: "text", Match: []string{"Processor model"}},
		"cpu_cores":     {Type: "int", Match: []string{"5563"}}, // par ID numérique
		"ram_gb":        {Type: "int", Match: []string{"Internal memory"}},
		"redundant_psu": {Type: "bool", Match: []string{"Redundant power supply (PSU)"}},
		"rack_units":    {Type: "int", Match: []string{"Rack capacity"}},
		"absent":        {Type: "int", Match: []string{"Does not exist"}},
	}}
	msg, err := buildMessage(&resp, "server", m)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Name != "DELL" || msg.Product.SKU != "R660-1234" || msg.Product.IcecatID != 12345 {
		t.Errorf("identité incorrecte : %+v", msg.Product)
	}
	if msg.Product.GTIN != "5397184657812" || msg.Product.ImageURL != "https://images.icecat.biz/img/500.jpg" || msg.Product.Source != "icecat" {
		t.Errorf("champs produit incorrects : %+v", msg.Product)
	}
	s := msg.Product.Server
	if s["cpu_model"] != "Intel Xeon Silver 4410Y" || s["cpu_cores"] != 12 || s["ram_gb"] != 32 || s["redundant_psu"] != true || s["rack_units"] != 1 {
		t.Errorf("projection incorrecte : %+v", s)
	}
	if _, ok := s["absent"]; ok {
		t.Errorf("une feature absente ne doit pas produire de colonne")
	}
	if len(msg.Product.RawSpecs) != 5 || msg.Product.RawSpecs[2].Unit != "GB" {
		t.Errorf("raw_specs incomplet : %+v", msg.Product.RawSpecs)
	}
	// Le contrat de message doit rester sérialisable tel que le consommateur l'attend.
	b, _ := json.Marshal(msg)
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back["product"].(map[string]any)["product_type"] != "server" {
		t.Errorf("product_type manquant dans le JSON")
	}
}

func TestHelpers(t *testing.T) {
	cases := map[string]int{"32 GB": 32, "2.5 GbE": 3, "1U": 1, "1,5 kg": 2}
	for in, want := range cases {
		if got, ok := toInt(in); !ok || got != want {
			t.Errorf("toInt(%q) = %d,%v ; attendu %d", in, got, ok, want)
		}
	}
	if _, ok := toInt("aucun"); ok {
		t.Errorf("toInt sans chiffre doit échouer")
	}
	for _, v := range []string{"Y", "yes", "Oui", "TRUE", "1"} {
		if !toBool(v) {
			t.Errorf("toBool(%q) attendu vrai", v)
		}
	}
	if toBool("N") || toBool("") {
		t.Errorf("toBool faux attendu")
	}
}

func TestPublicGTIN(t *testing.T) {
	for _, ok := range []string{"5902002360296", "5397184657812", "4006381333931", "012345678905"} {
		if !isPublicGTIN(ok) {
			t.Errorf("%s devrait être public", ok)
		}
	}
	for _, bad := range []string{"2000001403587", "5902002360295", "0000000000000", "12345", "abc"} {
		if isPublicGTIN(bad) {
			t.Errorf("%s devrait être rejeté", bad)
		}
	}
}
