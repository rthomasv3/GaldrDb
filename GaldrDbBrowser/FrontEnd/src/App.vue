<script setup>
import { ref, provide, onMounted } from "vue";
import { useRouter } from "vue-router";
import AppHeader from "./components/layout/AppHeader.vue";
import Sidebar from "./components/layout/Sidebar.vue";

const router = useRouter();

const database = ref({
    isOpen: false,
    filePath: null,
    stats: null
});

provide("database", database);

async function openDatabase() {
    const browseResult = await galdrInvoke("browseForDatabase");

    if (browseResult.filePath) {
        const openResult = await galdrInvoke("openDatabase", { filePath: browseResult.filePath });

        if (openResult.success) {
            database.value.isOpen = true;
            database.value.filePath = openResult.filePath;
            await refreshStats();
        }
    }
}

async function closeDatabase() {
    await galdrInvoke("closeDatabase");
    database.value.isOpen = false;
    database.value.filePath = null;
    database.value.stats = null;
    router.push("/");
}

async function refreshStats() {
    if (database.value.isOpen) {
        const stats = await galdrInvoke("getDatabaseStats");
        if (stats.success) {
            database.value.stats = stats;
        }
    }
}

provide("openDatabase", openDatabase);
provide("closeDatabase", closeDatabase);
provide("refreshStats", refreshStats);

onMounted(async () => {
    const status = await galdrInvoke("isDatabaseOpen");
    if (status.isOpen) {
        database.value.isOpen = true;
        database.value.filePath = status.filePath;
        await refreshStats();
    }
});
</script>

<template>
    <div class="app-layout">
        <AppHeader />
        <div class="app-body">
            <Sidebar v-if="database.isOpen" />
            <main class="app-content">
                <RouterView />
            </main>
        </div>
    </div>
</template>

<style scoped>
.app-layout {
    display: flex;
    flex-direction: column;
    height: 100%;
    min-height: 0;
    background-color: var(--bg-primary);
}

.app-body {
    display: flex;
    flex: 1;
    min-height: 0;
    overflow: hidden;
}

.app-content {
    flex: 1;
    overflow-y: auto;
    padding: 1.5rem 2rem;
    background-color: var(--bg-primary);
}
</style>
