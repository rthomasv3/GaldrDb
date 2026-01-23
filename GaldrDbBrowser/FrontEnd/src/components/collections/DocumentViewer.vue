<script setup>
import { computed, ref } from "vue";
import JsonTreeView from "./JsonTreeView.vue";

const props = defineProps({
    document: {
        type: Object,
        required: true
    }
});

const emit = defineEmits(["close"]);

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
                <button class="close-btn" @click="emit('close')">
                    &times;
                </button>
            </div>
        </div>

        <div class="viewer-content">
            <JsonTreeView v-if="viewMode === 'tree' && parsedJson" :data="parsedJson" />
            <pre v-else class="json-raw">{{ formattedJson }}</pre>
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
</style>
