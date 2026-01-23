<script setup>
import { inject } from "vue";

const database = inject("database");
const openDatabase = inject("openDatabase");
</script>

<template>
    <div class="home">
        <div v-if="!database.isOpen" class="welcome">
            <div class="welcome-content">
                <h2 class="welcome-title">Welcome to GaldrDb Browser</h2>
                <p class="welcome-description">
                    Open a database file to browse collections, view documents, and run queries.
                </p>
                <button class="btn btn-primary btn-large" @click="openDatabase">
                    Open Database
                </button>
            </div>
        </div>

        <div v-else class="dashboard">
            <h2 class="page-title">Dashboard</h2>
            <div class="cards-grid">
                <div class="card">
                    <h3 class="card-title">Collections</h3>
                    <p class="card-value">{{ database.stats?.collectionCount ?? 0 }}</p>
                </div>
                <div class="card">
                    <h3 class="card-title">Database Size</h3>
                    <p class="card-value">{{ formatBytes(database.stats?.fileSizeBytes ?? 0) }}</p>
                </div>
            </div>
        </div>
    </div>
</template>

<script>
function formatBytes(bytes) {
    if (bytes === 0) {
        return "0 B";
    }
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}
</script>

<style scoped>
.home {
    height: 100%;
}

.welcome {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
}

.welcome-content {
    text-align: center;
    max-width: 420px;
    padding: 2rem;
}

.welcome-title {
    font-size: 1.5rem;
    font-weight: 600;
    margin: 0 0 0.75rem 0;
    color: var(--text-primary);
    letter-spacing: -0.02em;
}

.welcome-description {
    color: var(--text-secondary);
    margin: 0 0 2rem 0;
    line-height: 1.6;
    font-size: 0.9375rem;
}

.btn-large {
    padding: 0.875rem 2rem;
    font-size: 0.9375rem;
    box-shadow: var(--shadow-md);
}

.btn-large:hover {
    transform: translateY(-1px);
    box-shadow: var(--shadow-lg);
}

.dashboard {
    max-width: 800px;
}

.page-title {
    font-size: 1.25rem;
    font-weight: 600;
    margin: 0 0 1.5rem 0;
    color: var(--text-primary);
}

.cards-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
}

.card {
    background-color: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 0.5rem;
    padding: 1.25rem;
    box-shadow: var(--shadow-sm);
}

.card-title {
    font-size: 0.8125rem;
    font-weight: 500;
    color: var(--text-muted);
    margin: 0 0 0.5rem 0;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

.card-value {
    font-size: 1.75rem;
    font-weight: 600;
    margin: 0;
    color: var(--text-primary);
    font-variant-numeric: tabular-nums;
}
</style>
