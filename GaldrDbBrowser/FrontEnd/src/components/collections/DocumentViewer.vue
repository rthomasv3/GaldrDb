<script setup>
import { computed, ref } from "vue";
import JsonTreeView from "./JsonTreeView.vue";

const props = defineProps({
    document: {
        type: Object,
        required: true
    }
});

const emit = defineEmits(["close", "edit", "delete"]);

const showDeleteConfirm = ref(false);
const deleting = ref(false);

async function handleDelete() {
    deleting.value = true;
    emit("delete", props.document.id);
}

function confirmDelete() {
    showDeleteConfirm.value = true;
}

function cancelDelete() {
    showDeleteConfirm.value = false;
}

const viewMode = ref("tree");

const parsedJson = computed(() => {
    try {
        return JSON.parse(props.document.json);
    } catch {
        return null;
    }
});

const formattedJson = computed(() => {
    try {
        return JSON.stringify(parsedJson.value, null, 2);
    } catch {
        return props.document.json;
    }
});
</script>

<template>
    <div class="document-viewer">
        <div class="viewer-header">
            <div class="viewer-title">
                <span>Document #{{ document.id }}</span>
            </div>
            <div class="viewer-actions">
                <div class="view-toggle">
                    <button
                        class="toggle-btn"
                        :class="{ active: viewMode === 'tree' }"
                        @click="viewMode = 'tree'"
                    >
                        Tree
                    </button>
                    <button
                        class="toggle-btn"
                        :class="{ active: viewMode === 'json' }"
                        @click="viewMode = 'json'"
                    >
                        JSON
                    </button>
                </div>
                <button class="btn btn-edit" @click="emit('edit', document)">
                    Edit
                </button>
                <button class="btn btn-delete" @click="confirmDelete">
                    Delete
                </button>
                <button class="close-btn" @click="emit('close')">
                    &times;
                </button>
            </div>
        </div>

        <div class="viewer-content">
            <JsonTreeView v-if="viewMode === 'tree' && parsedJson" :data="parsedJson" />
            <pre v-else class="json-raw">{{ formattedJson }}</pre>
        </div>

        <div v-if="showDeleteConfirm" class="delete-confirm-overlay">
            <div class="delete-confirm-dialog">
                <div class="dialog-title">Delete Document</div>
                <div class="dialog-message">
                    Are you sure you want to delete document #{{ document.id }}?
                    This action cannot be undone.
                </div>
                <div class="dialog-actions">
                    <button class="btn btn-secondary" @click="cancelDelete" :disabled="deleting">
                        Cancel
                    </button>
                    <button class="btn btn-danger" @click="handleDelete" :disabled="deleting">
                        {{ deleting ? "Deleting..." : "Delete" }}
                    </button>
                </div>
            </div>
        </div>
    </div>
</template>

<style scoped>
.document-viewer {
    flex: 1;
    min-width: 300px;
    max-width: 50%;
    display: flex;
    flex-direction: column;
    background-color: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 0.5rem;
    overflow: hidden;
    box-shadow: var(--shadow-md);
}

.viewer-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.625rem 1rem;
    border-bottom: 1px solid var(--border-color);
    background-color: var(--bg-tertiary);
}

.viewer-title {
    font-weight: 600;
    font-size: 0.8125rem;
    color: var(--text-primary);
}

.viewer-actions {
    display: flex;
    align-items: center;
    gap: 0.75rem;
}

.view-toggle {
    display: flex;
    background-color: var(--bg-primary);
    border-radius: 0.375rem;
    padding: 0.125rem;
    gap: 0.125rem;
}

.toggle-btn {
    padding: 0.25rem 0.75rem;
    background: transparent;
    border: none;
    border-radius: 0.25rem;
    color: var(--text-muted);
    font-size: 0.75rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s ease;
}

.toggle-btn:hover {
    color: var(--text-primary);
    background-color: var(--bg-tertiary);
}

.toggle-btn.active {
    background-color: var(--accent-color);
    color: white;
    box-shadow: var(--shadow-sm);
}

.close-btn {
    background: transparent;
    border: none;
    color: var(--text-muted);
    font-size: 1.25rem;
    line-height: 1;
    cursor: pointer;
    padding: 0.25rem;
    border-radius: 0.25rem;
    transition: all 0.15s ease;
}

.close-btn:hover {
    color: var(--text-primary);
    background-color: var(--bg-hover);
}

.viewer-content {
    flex: 1;
    overflow: auto;
    padding: 1rem;
    background-color: var(--bg-primary);
}

.json-raw {
    margin: 0;
    font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace;
    font-size: 0.8125rem;
    line-height: 1.6;
    color: var(--text-primary);
    white-space: pre-wrap;
    word-break: break-word;
}

.btn {
    padding: 0.25rem 0.625rem;
    border: none;
    border-radius: 0.25rem;
    font-size: 0.6875rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s ease;
}

.btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
}

.btn-edit {
    background-color: var(--accent-color);
    color: white;
}

.btn-edit:hover:not(:disabled) {
    background-color: var(--accent-hover);
}

.btn-delete {
    background-color: #dc2626;
    color: white;
    border: none;
}

.btn-delete:hover:not(:disabled) {
    background-color: #b91c1c;
}

.btn-secondary {
    background-color: var(--bg-primary);
    color: var(--text-primary);
    border: 1px solid var(--border-color);
}

.btn-secondary:hover:not(:disabled) {
    background-color: var(--bg-hover);
}

.btn-danger {
    background-color: #dc2626;
    color: white;
}

.btn-danger:hover:not(:disabled) {
    background-color: #b91c1c;
}

.delete-confirm-overlay {
    position: absolute;
    inset: 0;
    background-color: rgba(0, 0, 0, 0.5);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 10;
}

.delete-confirm-dialog {
    background-color: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 0.5rem;
    padding: 1.25rem;
    max-width: 320px;
    box-shadow: var(--shadow-lg);
}

.dialog-title {
    font-weight: 600;
    font-size: 0.9375rem;
    color: var(--text-primary);
    margin-bottom: 0.75rem;
}

.dialog-message {
    font-size: 0.8125rem;
    color: var(--text-secondary);
    margin-bottom: 1.25rem;
    line-height: 1.5;
}

.dialog-actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
}

.dialog-actions .btn {
    padding: 0.375rem 0.875rem;
    font-size: 0.75rem;
}
</style>
