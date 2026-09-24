# Importer Open Icecat — journal des transformations

Ce fichier répond à l'obligation de l'Open Content License d'Icecat : signaler
de façon visible que le contenu a été modifié, la nature des modifications et
leur date.

Source : fiches produits Open Icecat, obtenues via l'API JSON
(`live.icecat.biz/api`) et l'index `data.icecat.biz/export/freexml`.

Mention obligatoire, reprise dans l'interface :
**Database Right data-sheet [année] Icecat. All rights reserved.**
Les données sont fournies « en l'état », sans garantie d'aucune sorte.

## Transformations appliquées

| Transformation | Détail | Depuis |
|---|---|---|
| Sélection | Seules les catégories listées dans `ICECAT_CATEGORIES` sont importées. | 2026-09-18 |
| Sélection de champs | Un sous-ensemble de features est projeté sur le schéma ASIT selon `mapping.json`. Les autres ne sont pas affichées. | 2026-09-18 |
| Conservation intégrale | Toutes les features reçues sont conservées telles quelles dans `Product.raw_specs` (id, nom, valeur, unité), sans modification. | 2026-09-18 |
| Extraction numérique | Pour les colonnes entières, le premier nombre de la valeur est retenu (ex. `"32 GB"` → `32`). | 2026-09-18 |
| Normalisation booléenne | `Y`, `Yes`, `Oui`, `true`, `1` → vrai ; tout le reste → faux. | 2026-09-18 |
| Titre | `GeneralInfo.Title` est repris sans modification comme nom du produit. | 2026-09-18 |
| Image | Seule l'URL `Image.Pic500x500` (à défaut `HighPic`) est conservée. Les images ne sont pas retouchées. | 2026-09-18 |
| Constructeur | `GeneralInfo.Brand` alimente la table `Manufacturer`. | 2026-09-18 |

Aucune valeur n'est inventée ni corrigée. Une feature absente reste absente.

## Obligations associées

- `robots.txt` publié sur l'application, excluant les crawlers d'entraînement
  IA et d'agrégation (Fair Use Policy).
- Aucune utilisation du contenu pour l'entraînement de modèles.
- Volume : 1 million de fiches/mois maximum par dépôt pays/langue ;
  `ICECAT_MAX_PRODUCTS` et `ICECAT_DELAY_MS` maintiennent un rythme modéré.
