<script setup>
import { ref, watch, computed } from "vue";
import { useRoute } from "vue-router";
import DocumentList from "./DocumentList.vue";
import DocumentViewer from "./DocumentViewer.vue";
import DocumentEditor from "./DocumentEditor.vue";
import QueryPanel from "../query/QueryPanel.vue";

const route = useRoute();
const collectionName = computed(() => decodeURIComponent(route.params.name));
const collectionInfo = ref(null);
const queryResult = ref(null);
const selectedDocument = ref(null);
const editingDocument = ref(null);
const isCreating = ref(false);
const currentPage = ref(0);
const pageSize = 20;
const loading = ref(false);
const activeFilters = ref([]);

const showEditor = computed(() => isCreating.value || editingDocument.value !== null);

async function loadCollection() {
    loading.value = true;
    selectedDocument.value = null;
    activeFilters.value = [];
    currentPage.value = 0;

    try {
        collectionInfo.value = await galdrInvoke("getCollectionInfo", { name: collectionName.value });
        await loadDocuments();
    } finally {
        loading.value = false;
    }
}

async function loadDocuments() {
    queryResult.value = await galdrInvoke("queryDocuments", {
        request: {
            collection: collectionName.value,
            skip: currentPage.value * pageSize,
            limit: pageSize,
            filters: activeFilters.value.length > 0 ? activeFilters.value : null,
            orderByField: "Id",
            orderByDescending: false
        }
    });
}

async function handleSearch(filters) {
    activeFilters.value = filters;
    currentPage.value = 0;
    await loadDocuments();
}

async function selectDocument(doc) {
    const result = await galdrInvoke("getDocument", {
        collection: collectionName.value,
        id: doc.id
    });

    if (result.success) {
        selectedDocument.value = {
            id: result.id,
            json: result.json
        };
    }
}

function closeViewer() {
    selectedDocument.value = null;
}

function startCreate() {
    selectedDocument.value = null;
    editingDocument.value = null;
    isCreating.value = true;
}

function startEdit(doc) {
    editingDocument.value = doc;
    isCreating.value = false;
}

function cancelEdit() {
    editingDocument.value = null;
    isCreating.value = false;
}

async function handleSave(id) {
    const wasCreating = isCreating.value;
    editingDocument.value = null;
    isCreating.value = false;

    const result = await galdrInvoke("getDocument", {
        collection: collectionName.value,
        id: id
    });

    if (result.success) {
        selectedDocument.value = {
            id: result.id,
            json: result.json
        };

        if (wasCreating) {
            await loadDocuments();
            collectionInfo.value.documentCount++;
        } else {
            const docIndex = queryResult.value.documents.findIndex(d => d.id === id);
            if (docIndex !== -1) {
                queryResult.value.documents[docIndex] = {
                    id: result.id,
                    json: result.json
                };
            }
        }
    }
}

async function handleDelete(id) {
    const result = await galdrInvoke("deleteDocument", {
        collection: collectionName.value,
        id: id
    });

    if (result.success) {
        selectedDocument.value = null;
        await loadCollection();
    }
}

async function goToPage(page) {
    currentPage.value = page;
    await loadDocuments();
}

watch(collectionName, loadCollection, { immediate: true });

const totalPages = computed(() => {
    if (!queryResult.value) {
        return 0;
    }
    return Math.ceil(queryResult.value.totalCount / pageSize);
});
</script>

<template>
    <div class="collection-view">
        <div class="collection-header">
            <div class="header-info">
                <h2 class="collection-title">{{ collectionName }}</h2>
                <span v-if="collectionInfo" class="doc-count">
                    {{ collectionInfo.documentCount }} documents
                </span>
            </div>
            <div class="header-actions">
                <button class="btn btn-primary" @click="startCreate">
                    + New Document
                </button>
            </div>
        </div>

        <div v-if="collectionInfo && collectionInfo.indexes.length > 0" class="indexes-info">
            <span class="indexes-label">Indexes:</span>
            <span v-for="idx in collectionInfo.indexes" :key="idx.fieldName" class="index-badge">
                {{ idx.fieldName }}
            </span>
        </div>

        <QueryPanel
            v-if="collectionInfo"
            :indexes="collectionInfo.indexes"
            @search="handleSearch"
        />

        <div v-if="activeFilters.length > 0" class="active-filters">
            <span class="filters-label">Active filters:</span>
            <span v-for="(filter, idx) in activeFilters" :key="idx" class="filter-badge">
                {{ filter.field }} {{ filter.op }} {{ filter.value }}
                <span v-if="filter.value2"> and {{ filter.value2 }}</span>
            </span>
        </div>

        <div class="collection-content">
            <div class="documents-panel" :class="{ 'with-viewer': selectedDocument || showEditor }">
                <DocumentList
                    v-if="queryResult && queryResult.success"
                    :documents="queryResult.documents"
                    :selected-id="selectedDocument?.id"
                    :current-page="currentPage"
                    :total-pages="totalPages"
                    @select="selectDocument"
                    @page-change="goToPage"
                />
                <div v-else-if="loading" class="loading">Loading...</div>
                <div v-else class="empty">No documents found</div>
            </div>

            <DocumentEditor
                v-if="showEditor"
                :document="editingDocument"
                :collection-name="collectionName"
                @save="handleSave"
                @cancel="cancelEdit"
            />

            <DocumentViewer
                v-else-if="selectedDocument"
                :document="selectedDocument"
                @close="closeViewer"
                @edit="startEdit"
                @delete="handleDelete"
            />
        </div>
    </div>
</template>

<style scoped>
.collection-view {
    display: flex;
    flex-direction: column;
    height: 100%;
}

.collection-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1rem;
    gap: 1rem;
}

.header-actions {
    display: flex;
    gap: 0.5rem;
}

.btn {
    padding: 0.375rem 0.875rem;
    border: none;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s ease;
}

.btn-primary {
    background-color: var(--accent-color);
    color: white;
}

.btn-primary:hover {
    background-color: var(--accent-hover);
}

.header-info {
    display: flex;
    align-items: baseline;
    gap: 0.875rem;
}

.collection-title {
    font-size: 1.125rem;
    font-weight: 600;
    margin: 0;
    color: var(--text-primary);
    letter-spacing: -0.01em;
}

.doc-count {
    color: var(--text-muted);
    font-size: 0.8125rem;
    font-variant-numeric: tabular-nums;
}

.indexes-info {
    display: flex;
    align-items: center;
    gap: 0.375rem;
    flex-wrap: wrap;
    margin-bottom: 1rem;
}

.indexes-label {
    color: var(--text-muted);
    font-size: 0.6875rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-right: 0.25rem;
}

.index-badge {
    background-color: var(--bg-tertiary);
    padding: 0.1875rem 0.5rem;
    border-radius: 0.25rem;
    font-size: 0.6875rem;
    color: var(--text-primary);
    font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace;
    border: 1px solid var(--border-color);
}

.collection-content {
    display: flex;
    flex: 1;
    gap: 1rem;
    min-height: 0;
    overflow: hidden;
}

.documents-panel {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    display: flex;
    flex-direction: column;
}

.documents-panel.with-viewer {
    max-width: 50%;
}

.loading,
.empty {
    color: var(--text-muted);
    padding: 3rem 2rem;
    text-align: center;
    font-size: 0.875rem;
}

.active-filters {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 1rem;
    flex-wrap: wrap;
}

.filters-label {
    color: var(--text-muted);
    font-size: 0.6875rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}

.filter-badge {
    background-color: var(--accent-color);
    color: white;
    padding: 0.25rem 0.625rem;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace;
    box-shadow: var(--shadow-sm);
}
</style>
