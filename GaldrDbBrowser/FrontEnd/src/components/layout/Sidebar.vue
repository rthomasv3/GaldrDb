<script setup>
import { inject, ref, watch } from "vue";
import { useRouter, useRoute } from "vue-router";

const router = useRouter();
const route = useRoute();
const database = inject("database");
const collections = ref([]);

async function loadCollections() {
    if (database.value.isOpen) {
        const result = await galdrInvoke("getCollections");
        collections.value = result.collections || [];
    } else {
        collections.value = [];
    }
}

watch(() => database.value.isOpen, loadCollections, { immediate: true });

function formatBytes(bytes) {
    if (bytes === 0) {
        return "0 B";
    }
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function navigateToCollection(name) {
    router.push(`/collection/${encodeURIComponent(name)}`);
}

function isActiveCollection(name) {
    return route.params.name === name;
}
</script>

<template>
    <aside class="sidebar">
        <div class="sidebar-section">
            <h2 class="section-title">Database</h2>
            <div v-if="database.stats" class="stats-grid">
                <div class="stat-item">
                    <span class="stat-label">Collections</span>
                    <span class="stat-value">{{ database.stats.collectionCount }}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">File Size</span>
                    <span class="stat-value">{{ formatBytes(database.stats.fileSizeBytes) }}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Page Size</span>
                    <span class="stat-value">{{ formatBytes(database.stats.pageSize) }}</span>
                </div>
            </div>
        </div>

        <div class="sidebar-section">
            <h2 class="section-title">Collections</h2>
            <div v-if="collections.length > 0" class="collection-list">
                <button
                    v-for="col in collections"
                    :key="col.name"
                    class="collection-item"
                    :class="{ active: isActiveCollection(col.name) }"
                    @click="navigateToCollection(col.name)"
                >
                    <span class="collection-name">{{ col.name }}</span>
                    <span class="collection-count">{{ col.documentCount }}</span>
                </button>
            </div>
            <p v-else class="placeholder-text">No collections</p>
        </div>
    </aside>
</template>

<style scoped>
.sidebar {
    width: 256px;
    min-width: 256px;
    background-color: var(--bg-secondary);
    border-right: 1px solid var(--border-color);
    padding: 1.25rem;
    overflow-y: auto;
}

.sidebar-section {
    margin-bottom: 2rem;
}

.section-title {
    font-size: 0.6875rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text-muted);
    margin: 0 0 0.875rem 0;
    padding-left: 0.25rem;
}

.stats-grid {
    display: flex;
    flex-direction: column;
    gap: 0.625rem;
    background-color: var(--bg-primary);
    padding: 0.875rem;
    border-radius: 0.5rem;
    border: 1px solid var(--border-subtle);
}

.stat-item {
    display: flex;
    justify-content: space-between;
    font-size: 0.8125rem;
}

.stat-label {
    color: var(--text-muted);
}

.stat-value {
    color: var(--text-primary);
    font-weight: 500;
    font-variant-numeric: tabular-nums;
}

.placeholder-text {
    color: var(--text-muted);
    font-size: 0.8125rem;
    margin: 0;
    padding: 0.5rem 0.75rem;
}

.collection-list {
    display: flex;
    flex-direction: column;
    gap: 0.125rem;
}

.collection-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 0.75rem;
    background: transparent;
    border: none;
    border-radius: 0.375rem;
    color: var(--text-primary);
    font-size: 0.8125rem;
    text-align: left;
    cursor: pointer;
    transition: all 0.15s ease;
}

.collection-item:hover {
    background-color: var(--bg-tertiary);
}

.collection-item.active {
    background-color: var(--accent-color);
    box-shadow: var(--shadow-sm);
}

.collection-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}

.collection-count {
    color: var(--text-primary);
    font-size: 0.6875rem;
    font-weight: 500;
    flex-shrink: 0;
    margin-left: 0.75rem;
    padding: 0.125rem 0.375rem;
    background-color: var(--bg-tertiary);
    border-radius: 0.25rem;
}

.collection-item:hover .collection-count {
    background-color: var(--bg-hover);
}

.collection-item.active .collection-count {
    background-color: rgba(255, 255, 255, 0.2);
    color: rgba(255, 255, 255, 0.9);
}
</style>
