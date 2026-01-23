<script setup>
import { ref, computed } from "vue";

const props = defineProps({
    data: {
        type: [Object, Array, String, Number, Boolean, null],
        required: true
    },
    keyName: {
        type: String,
        default: null
    },
    depth: {
        type: Number,
        default: 0
    }
});

const expanded = ref(props.depth < 2);

const isObject = computed(() => props.data !== null && typeof props.data === "object" && !Array.isArray(props.data));
const isArray = computed(() => Array.isArray(props.data));
const isExpandable = computed(() => isObject.value || isArray.value);

const entries = computed(() => {
    if (isArray.value) {
        return props.data.map((val, idx) => ({ key: idx, value: val }));
    }
    if (isObject.value) {
        return Object.entries(props.data).map(([key, value]) => ({ key, value }));
    }
    return [];
});

const valueType = computed(() => {
    if (props.data === null) {
        return "null";
    }
    if (isArray.value) {
        return "array";
    }
    return typeof props.data;
});

const displayValue = computed(() => {
    if (props.data === null) {
        return "null";
    }
    if (typeof props.data === "string") {
        return `"${props.data}"`;
    }
    if (typeof props.data === "boolean") {
        return props.data ? "true" : "false";
    }
    return String(props.data);
});

function toggle() {
    if (isExpandable.value) {
        expanded.value = !expanded.value;
    }
}
</script>

<template>
    <div class="tree-node" :style="{ paddingLeft: depth > 0 ? '1rem' : '0' }">
        <div class="node-row" @click="toggle">
            <span v-if="isExpandable" class="expand-icon">
                {{ expanded ? "▼" : "▶" }}
            </span>
            <span v-else class="expand-placeholder"></span>

            <span v-if="keyName !== null" class="node-key">{{ keyName }}:</span>

            <template v-if="isExpandable">
                <span class="node-bracket">{{ isArray ? "[" : "{" }}</span>
                <span v-if="!expanded" class="collapsed-preview">
                    {{ isArray ? `${entries.length} items` : `${entries.length} keys` }}
                </span>
                <span v-if="!expanded" class="node-bracket">{{ isArray ? "]" : "}" }}</span>
            </template>
            <template v-else>
                <span class="node-value" :class="valueType">{{ displayValue }}</span>
            </template>
        </div>

        <div v-if="isExpandable && expanded" class="node-children">
            <JsonTreeView
                v-for="entry in entries"
                :key="entry.key"
                :data="entry.value"
                :key-name="String(entry.key)"
                :depth="depth + 1"
            />
            <div class="closing-bracket" :style="{ paddingLeft: '0' }">
                <span class="node-bracket">{{ isArray ? "]" : "}" }}</span>
            </div>
        </div>
    </div>
</template>

<style scoped>
.tree-node {
    font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace;
    font-size: 0.8125rem;
    line-height: 1.7;
}

.node-row {
    display: flex;
    align-items: flex-start;
    gap: 0.25rem;
    cursor: default;
    padding: 0.0625rem 0;
    border-radius: 0.25rem;
}

.node-row:hover {
    background-color: rgba(255, 255, 255, 0.03);
}

.expand-icon {
    width: 1rem;
    flex-shrink: 0;
    cursor: pointer;
    color: var(--text-muted);
    font-size: 0.5rem;
    user-select: none;
    display: flex;
    align-items: center;
    justify-content: center;
    height: 1.4em;
    transition: color 0.15s ease;
}

.expand-icon:hover {
    color: var(--text-primary);
}

.expand-placeholder {
    width: 1rem;
    flex-shrink: 0;
}

.node-key {
    color: #7dd3fc;
}

.node-bracket {
    color: var(--text-muted);
}

.collapsed-preview {
    color: var(--text-muted);
    font-style: italic;
    margin: 0 0.375rem;
    font-size: 0.75rem;
}

.node-value {
    word-break: break-word;
}

.node-value.string {
    color: #fda4af;
}

.node-value.number {
    color: #86efac;
}

.node-value.boolean {
    color: #c4b5fd;
}

.node-value.null {
    color: #94a3b8;
    font-style: italic;
}

.node-children {
    border-left: 1px solid var(--border-color);
    margin-left: 0.5rem;
}

.closing-bracket {
    padding-left: 1rem;
}
</style>
