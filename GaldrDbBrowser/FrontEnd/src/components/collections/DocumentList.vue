<script setup>
import { computed } from "vue";

const props = defineProps({
    documents: {
        type: Array,
        required: true
    },
    selectedId: {
        type: Number,
        default: null
    },
    currentPage: {
        type: Number,
        default: 0
    },
    totalPages: {
        type: Number,
        default: 0
    }
});

const emit = defineEmits(["select", "page-change"]);

function getPreview(doc) {
    try {
        const parsed = JSON.parse(doc.json);
        const keys = Object.keys(parsed).filter(k => k !== "_id").slice(0, 3);
        const preview = keys.map(k => {
            let value = parsed[k];
            if (typeof value === "string" && value.length > 30) {
                value = value.substring(0, 30) + "...";
            } else if (typeof value === "object") {
                value = Array.isArray(value) ? `[${value.length}]` : "{...}";
            }
            return `${k}: ${value}`;
        }).join(", ");
        return preview || "(empty)";
    } catch {
        return "(invalid JSON)";
    }
}

const canGoPrev = computed(() => props.currentPage > 0);
const canGoNext = computed(() => props.currentPage < props.totalPages - 1);
</script>

<template>
    <div class="document-list">
        <div class="list-content pr-2">
            <button
                v-for="doc in documents"
                :key="doc.id"
                class="document-item"
                :class="{ selected: doc.id === selectedId }"
                @click="emit('select', doc)"
            >
                <span class="doc-id">#{{ doc.id }}</span>
                <span class="doc-preview">{{ getPreview(doc) }}</span>
            </button>
        </div>

        <div v-if="totalPages > 1" class="pagination">
            <button
                class="btn btn-secondary btn-sm"
                :disabled="!canGoPrev"
                @click="emit('page-change', currentPage - 1)"
            >
                Prev
            </button>
            <span class="page-info">
                Page {{ currentPage + 1 }} of {{ totalPages }}
            </span>
            <button
                class="btn btn-secondary btn-sm"
                :disabled="!canGoNext"
                @click="emit('page-change', currentPage + 1)"
            >
                Next
            </button>
        </div>
    </div>
</template>

<style scoped>
.document-list {
    display: flex;
    flex-direction: column;
    height: 100%;
}

.list-content {
    flex: 1;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 0.375rem;
}

.document-item {
    display: flex;
    align-items: center;
    gap: 0.875rem;
    padding: 0.75rem 1rem;
    background-color: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 0.375rem;
    cursor: pointer;
    text-align: left;
    transition: all 0.15s ease;
}

.document-item:hover {
    background-color: var(--bg-tertiary);
    border-color: var(--bg-hover);
}

.document-item.selected {
    background-color: var(--accent-color);
    border-color: var(--accent-color);
    box-shadow: var(--shadow-sm);
}

.doc-id {
    font-weight: 600;
    font-size: 0.8125rem;
    color: var(--text-muted);
    flex-shrink: 0;
    font-variant-numeric: tabular-nums;
    min-width: 4rem;
}

.document-item:hover .doc-id {
    color: var(--text-secondary);
}

.document-item.selected .doc-id {
    color: rgba(255, 255, 255, 0.85);
}

.doc-preview {
    font-size: 0.8125rem;
    color: var(--text-secondary);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace;
}

.document-item.selected .doc-preview {
    color: rgba(255, 255, 255, 0.95);
}

.pagination {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    padding-top: 0.75rem;
    border-top: 1px solid var(--border-color);
    margin-top: 0.75rem;
    flex-wrap: nowrap;
}

.page-info {
    color: var(--text-muted);
    font-size: 0.75rem;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
}

@media (max-width: 900px) {
    .pagination {
        gap: 0.375rem;
    }

    .btn-sm {
        padding: 0.25rem 0.5rem;
        font-size: 0.75rem;
    }

    .page-info {
        font-size: 0.6875rem;
    }
}
</style>
