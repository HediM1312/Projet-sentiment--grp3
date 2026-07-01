# Note de cadrage éthique et RGPD — Projet 3

## Sources de données

| Source | Type | Accès | Données personnelles |
|--------|------|-------|----------------------|
| Reddit (API JSON publique) | Posts publics | Sans clé, rate-limit 1 req/2s | Pseudonyme auteur |
| RSS presse sportive | Articles | Public | Aucune |

## Principes appliqués

### Minimisation des données
- Seuls le texte, la source, l'horodatage et un identifiant de déduplication (hash MD5 du contenu) sont conservés.
- Aucun profilage individuel : toutes les analyses sont **agrégées** (par heure, par sujet, par sentiment).

### Anonymisation
- Le champ `author` (pseudonyme Reddit) est collecté uniquement pour la déduplication technique.
- Il n'est **pas** stocké dans les tables Gold ni affiché dans le dashboard.

### Consentement et données publiques
- Seuls des contenus explicitement publics (subreddits ouverts, flux RSS) sont collectés.
- Aucun scraping de comptes privés, de DMs, ou de plateformes nécessitant une authentification utilisateur.

### Durée de conservation
- Bronze : 30 jours glissants (purge automatique à prévoir en Phase 2).
- Silver/Gold : données agrégées anonymisées, conservation illimitée pour le projet.

### Interdictions explicites
- Pas de reconstruction de profil individuel.
- Pas de détection d'appartenance politique, religieuse ou ethnique.
- Pas de revente ou partage des données brutes hors du groupe.

## Références
- RGPD Art. 5 (principes relatifs au traitement)
- CNIL — Lignes directrices sur le scraping (2023)
