# Weather_data

Ce projet permet de suivre et comparer les conditions météorologiques de plusieurs villes à travers un pipeline ETL automatisé, une modélisation en étoile, une analyse exploratoire et des visualisations interactives.

## Structure du projet

- **ETL/dags/**  
  Contient le DAG Airflow pour l’extraction, la transformation et le chargement (ETL) des données météorologiques récentes.  
  Le modèle en étoile généré est stocké dans :


- **Historical_data/**  
Contient les données historiques ainsi que le script permettant leur sauvegarde au format CSV.

- **Data_merge/**  
Contient les données finales à utiliser pour les analyses.  
- Le fichier `meteo_global.csv` regroupe les données consolidées.
- Le modèle en étoile utilisé pour les visualisations se trouve dans :
  ```
  Data_merge/data/star_schema/
  ```

- **Racine du projet**  
On y trouve les notebooks ou scripts dédiés à :
- L’analyse exploratoire des données (EDA)
- La visualisation des indicateurs météo à l’aide de dashboards interactifs (Power BI, etc.)

## Résumé des composants

| Composant              | Contenu principal                                  |
|------------------------|----------------------------------------------------|
| `ETL/dags/`            | DAG Airflow pour ETL des données récentes         |
| `Historical_data/`     | Données historiques + script de sauvegarde        |
| `Data_merge/`          | Données finales consolidées + modèle en étoile    |
| Racine du projet       | EDA + visualisation interactive                   |

---

*Cette structure permet de gérer efficacement les flux de données météorologiques, de maintenir un historique propre et d’exploiter les données à travers des analyses visuelles pertinentes.*
