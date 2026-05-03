# Visualisation, Qualité et Gouvernance

Ce projet collecte des offres d'emploi. Dans le sujet, le mot "articles" correspond ici aux enregistrements collectés depuis les sources (`Rekrute`, `Emploi.ma`, `Indeed`).

## 8. Visualisation

Les tableaux de bord sont créés dans Metabase à partir du schéma PostgreSQL `dw`.

Vues disponibles pour les dashboards :

| Dashboard | Vue SQL | Objectif |
|-----------|---------|----------|
| Tendances d'actualité | `dw.v_dashboard_trends` | Evolution quotidienne du nombre d'offres par source |
| Nombre articles/offres par source | `dw.v_dashboard_source_counts` | Volume total, 7 jours, 30 jours par source |
| Mots clés les plus fréquents | `dw.v_dashboard_top_keywords` | Compétences les plus mentionnées dans les offres |
| Qualité des données | `dw.v_dashboard_quality_summary` | Taux de réussite des contrôles qualité |
| Traçabilité pipeline | `dw.v_governance_pipeline_runs` | Historique des exécutions Airflow |

Accès Metabase : `http://localhost:3000`

Connexion PostgreSQL depuis Metabase :

| Paramètre | Valeur |
|-----------|--------|
| Host | `postgres` |
| Port | `5432` |
| Database | `job_market` |
| User | `jobadmin` |
| Password | `jobpassword123` |

## 9. Qualité des données

Les contrôles sont exécutés dans `quality/data_quality_checks.py` et stockés dans `dw.quality_reports`.

Tests principaux :

| Test | Check | Dimension |
|------|-------|-----------|
| Article/offre sans titre | `silver_title_not_null` | Complétude |
| Date manquante | `silver_date_not_null` | Complétude |
| Date invalide | `silver_date_format_valid` | Validité |
| Contenu trop court | `silver_description_sufficient` | Validité |
| URL manquante | `silver_url_not_null` | Complétude |
| URL dupliquée | `silver_url_unique` | Cohérence |
| Type de contrat invalide | `silver_contract_type_valid` | Validité |

Dimensions couvertes :

| Dimension | Exemple |
|-----------|---------|
| Complétude | titre, URL, date, ville, société, secteur |
| Cohérence | unicité des URL, cohérence des agrégations |
| Validité | format des dates, valeurs de contrat, contenu minimum |

## 10. Gouvernance

La gouvernance est assurée par :

| Élément | Implémentation |
|---------|----------------|
| Dictionnaire des données | `DATA_DICTIONARY.md` |
| Traçabilité des données | `LINEAGE.md` |
| Identifiant stable | `uid = MD5(url)` |
| Traçabilité d'exécution | `pipeline_run_id` |
| Historique des runs | `dw.pipeline_runs` |
| Rapports qualité | `dw.quality_reports` |
| Données append-only | Bucket MinIO `bronze` |
| Données nettoyées | Bucket MinIO `silver` |
| Données analytiques | Bucket MinIO `gold` + tables PostgreSQL |

Chaque enregistrement conserve `source`, `scraped_at`, `pipeline_run_id` et `processed_at`, ce qui permet de retrouver son origine et le run Airflow qui l'a produit.
