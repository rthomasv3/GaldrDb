<script setup>
import { ref, computed } from "vue";

const props = defineProps({
    indexes: {
        type: Array,
        default: () => []
    }
});

const emit = defineEmits(["search"]);

const activeTab = ref("simple");
const simpleField = ref("Id");
const simpleOp = ref("eq");
const simpleValue = ref("");

const filters = ref([]);

const operators = [
    { value: "eq", label: "=" },
    { value: "neq", label: "!=" },
    { value: "gt", label: ">" },
    { value: "gte", label: ">=" },
    { value: "lt", label: "<" },
    { value: "lte", label: "<=" },
    { value: "startsWith", label: "starts with" },
    { value: "endsWith", label: "ends with" },
    { value: "contains", label: "contains" },
    { value: "between", label: "between" }
];

const availableFields = computed(() => {
    const fields = [{ name: "Id", type: "Int32" }];
    if (props.indexes) {
        props.indexes.forEach(idx => {
            if (!fields.some(f => f.name === idx.fieldName)) {
                fields.push({ name: idx.fieldName, type: idx.fieldType });
            }
        });
    }
    return fields;
});

function addFilter() {
    filters.value.push({
        id: Date.now(),
        field: "Id",
        op: "eq",
        value: "",
        value2: ""
    });
}

function removeFilter(id) {
    filters.value = filters.value.filter(f => f.id !== id);
}

function executeSimpleSearch() {
    if (!simpleValue.value.trim()) {
        emit("search", []);
        return;
    }

    const searchFilters = [{
        field: simpleField.value,
        op: simpleOp.value,
        value: simpleValue.value,
        value2: null
    }];
    emit("search", searchFilters);
}

function executeBuilderSearch() {
    const searchFilters = filters.value
        .filter(f => f.value.trim())
        .map(f => ({
            field: f.field,
            op: f.op,
            value: f.value,
            value2: f.op === "between" ? f.value2 : null
        }));
    emit("search", searchFilters);
}

function clearSearch() {
    simpleValue.value = "";
    filters.value = [];
    emit("search", []);
}
</script>

<template>
    <div class="query-panel">
        <div class="tabs">
            <button
                :class="['tab', { active: activeTab === 'simple' }]"
                @click="activeTab = 'simple'"
            >
                Simple Search
            </button>
            <button
                :class="['tab', { active: activeTab === 'builder' }]"
                @click="activeTab = 'builder'"
            >
                Query Builder
            </button>
        </div>

        <div class="tab-content">
            <div v-if="activeTab === 'simple'" class="simple-search">
                <select v-model="simpleField" class="field-select">
                    <option v-for="field in availableFields" :key="field.name" :value="field.name">
                        {{ field.name }}
                    </option>
                </select>
                <select v-model="simpleOp" class="op-select">
                    <option v-for="op in operators" :key="op.value" :value="op.value">
                        {{ op.label }}
                    </option>
                </select>
                <input
                    v-model="simpleValue"
                    type="text"
                    class="value-input"
                    placeholder="Value..."
                    @keyup.enter="executeSimpleSearch"
                />
                <button class="btn btn-primary" @click="executeSimpleSearch">Search</button>
                <button class="btn btn-secondary" @click="clearSearch">Clear</button>
            </div>

            <div v-else class="query-builder">
                <div class="filters-list">
                    <div v-for="filter in filters" :key="filter.id" class="filter-row">
                        <select v-model="filter.field" class="field-select">
                            <option v-for="field in availableFields" :key="field.name" :value="field.name">
                                {{ field.name }}
                            </option>
                        </select>
                        <select v-model="filter.op" class="op-select">
                            <option v-for="op in operators" :key="op.value" :value="op.value">
                                {{ op.label }}
                            </option>
                        </select>
                        <input
                            v-model="filter.value"
                            type="text"
                            class="value-input"
                            placeholder="Value..."
                        />
                        <input
                            v-if="filter.op === 'between'"
                            v-model="filter.value2"
                            type="text"
                            class="value-input"
                            placeholder="Value 2..."
                        />
                        <button class="btn btn-icon" @click="removeFilter(filter.id)" title="Remove filter">
                            X
                        </button>
                    </div>
                    <div v-if="filters.length === 0" class="no-filters">
                        No filters added. Click "Add Filter" to start building your query.
                    </div>
                </div>
                <div class="builder-actions">
                    <button class="btn btn-secondary" @click="addFilter">Add Filter</button>
                    <button class="btn btn-primary" @click="executeBuilderSearch">Search</button>
                    <button class="btn btn-secondary" @click="clearSearch">Clear</button>
                </div>
            </div>
        </div>
    </div>
</template>

<style scoped>
.query-panel {
    background-color: var(--bg-secondary);
    border-radius: 0.5rem;
    border: 1px solid var(--border-color);
    margin-bottom: 1rem;
    box-shadow: var(--shadow-sm);
}

.tabs {
    display: flex;
    border-bottom: 1px solid var(--border-color);
    padding: 0 0.5rem;
}

.tab {
    padding: 0.625rem 1rem;
    background: none;
    border: none;
    color: var(--text-secondary);
    cursor: pointer;
    font-size: 0.8125rem;
    font-weight: 500;
    border-bottom: 2px solid transparent;
    margin-bottom: -1px;
    transition: all 0.15s ease;
}

.tab:hover {
    color: var(--text-primary);
}

.tab.active {
    color: #60a5fa;
    border-bottom-color: #60a5fa;
}

.tab-content {
    padding: 1rem;
}

.simple-search {
    display: flex;
    flex-direction: row;
    gap: 0.5rem;
    align-items: center;
    flex-wrap: nowrap;
}

.field-select,
.op-select,
.value-input {
    padding: 0.5rem 0.75rem;
    border: 1px solid var(--border-color);
    border-radius: 0.375rem;
    background-color: var(--bg-primary);
    color: var(--text-primary);
    font-size: 0.8125rem;
    transition: border-color 0.15s ease, box-shadow 0.15s ease;
}

.field-select:focus,
.op-select:focus,
.value-input:focus {
    outline: none;
    border-color: var(--accent-color);
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
}

.field-select {
    min-width: 120px;
}

.op-select {
    min-width: 110px;
}

.value-input {
    flex: 1;
    min-width: 150px;
}

.value-input::placeholder {
    color: var(--text-muted);
}

.btn {
    padding: 0.5rem 1rem;
    border: none;
    border-radius: 0.375rem;
    cursor: pointer;
    font-size: 0.8125rem;
    font-weight: 500;
    white-space: nowrap;
    transition: all 0.15s ease;
}

.btn-primary {
    background-color: var(--accent-color);
    color: white;
}

.btn-primary:hover {
    background-color: var(--accent-hover);
}

.btn-secondary {
    background-color: var(--bg-tertiary);
    color: var(--text-primary);
}

.btn-secondary:hover {
    background-color: var(--bg-hover);
}

.btn-icon {
    padding: 0.5rem;
    background-color: var(--bg-tertiary);
    color: var(--text-muted);
    width: 32px;
    height: 32px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 0.375rem;
}

.btn-icon:hover {
    background-color: var(--bg-hover);
    color: var(--error);
}

.query-builder {
    display: flex;
    flex-direction: column;
    gap: 1rem;
}

.filters-list {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
}

.filter-row {
    display: flex;
    gap: 0.5rem;
    align-items: center;
}

.no-filters {
    color: var(--text-muted);
    font-size: 0.8125rem;
    padding: 0.75rem;
    background-color: var(--bg-primary);
    border-radius: 0.375rem;
    border: 1px dashed var(--border-color);
    text-align: center;
}

.builder-actions {
    display: flex;
    gap: 0.5rem;
}
</style>
