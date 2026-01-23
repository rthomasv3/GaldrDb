<script setup>
import { inject } from "vue";

const database = inject("database");
const openDatabase = inject("openDatabase");
const closeDatabase = inject("closeDatabase");

function getFileName(filePath) {
    if (!filePath) {
        return "";
    }
    const parts = filePath.split(/[/\\]/);
    return parts[parts.length - 1];
}
</script>

<template>
    <header class="app-header">
        <div class="header-left">
            <h1 class="app-title">GaldrDb Browser</h1>
            <span v-if="database.isOpen" class="file-name">{{ getFileName(database.filePath) }}</span>
        </div>
        <div class="header-right">
            <button v-if="!database.isOpen" class="btn btn-primary" @click="openDatabase">
                Open Database
            </button>
            <template v-else>
                <button class="btn btn-secondary" @click="openDatabase">
                    Open
                </button>
                <button class="btn btn-secondary" @click="closeDatabase">
                    Close
                </button>
            </template>
        </div>
    </header>
</template>

<style scoped>
.app-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0 1.5rem;
    height: 3.5rem;
    background-color: var(--bg-secondary);
    border-bottom: 1px solid var(--border-color);
    box-shadow: var(--shadow-sm);
}

.header-left {
    display: flex;
    align-items: center;
    gap: 1rem;
}

.app-title {
    font-size: 1rem;
    font-weight: 600;
    margin: 0;
    color: var(--text-primary);
    letter-spacing: -0.01em;
}

.file-name {
    color: var(--text-primary);
    font-size: 0.8125rem;
    padding: 0.25rem 0.625rem;
    background-color: var(--bg-tertiary);
    border-radius: 0.25rem;
    border: 1px solid var(--border-color);
}

.header-right {
    display: flex;
    gap: 0.5rem;
}
</style>
