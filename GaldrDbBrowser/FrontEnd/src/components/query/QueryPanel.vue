<script setup>
import { ref, computed } from "vue";
import Combobox from "../common/Combobox.vue";

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
    const fields = ["Id"];
    if (props.indexes) {
        props.indexes.forEach(idx => {
            if (!fields.includes(idx.fieldName)) {
                fields.push(idx.fieldName);
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
                <Combobox
                    v-model="simpleField"
                    :options="availableFields"
                    placeholder="Field..."
                    class="field-combobox"
                />
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
                        <Combobox
                            v-model="filter.field"
                            :options="availableFields"
                            placeholder="Field..."
                            class="field-combobox"
                        />
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
                        <button class="btn btn-remove" @click="removeFilter(filter.id)" title="Remove filter">
                            &times;
                        </button>
                    </div>
                    <div v-if="filters.length === 0" class="no-filters">
                        No filters added. Click "Add Filter" to start building your query.
                    </div>
                </div>
                <div class="builder-actions">
                    <button class="btn btn-secondary" @click="addFilter">Add Filter</button>
                    <div class="builder-actions-right">
                        <button class="btn btn-primary" @click="executeBuilderSearch">Search</button>
                        <button class="btn btn-secondary" @click="clearSearch">Clear</button>
                    </div>
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

@media (max-width: 900px) {
    .simple-search {
        flex-wrap: wrap;
    }

    .simple-search .value-input {
        flex-basis: 100%;
    }

    .simple-search .btn {
        order: 10;
    }

    .simple-search .btn:first-of-type {
        margin-left: auto;
    }
}

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

.op-select:focus,
.value-input:focus {
    outline: none;
    border-color: var(--accent-color);
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
}

.field-combobox {
    width: 180px;
    flex-shrink: 0;
}

.op-select {
    width: auto;
    min-width: 140px;
    flex-shrink: 0;
}

.value-input {
    flex: 1;
    min-width: 120px;
}

.value-input::placeholder {
    color: var(--text-muted);
}

.btn-remove {
    padding: 0;
    background-color: #dc2626;
    color: white;
    width: 28px;
    height: 28px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 0.375rem;
    font-size: 1.125rem;
    line-height: 1;
    flex-shrink: 0;
}

.btn-remove:hover {
    background-color: #b91c1c;
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
    max-height: 125px;
    overflow-y: auto;
    padding-right: 0.5rem;
}

.filter-row {
    display: flex;
    gap: 0.5rem;
    align-items: center;
    flex-wrap: nowrap;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--border-color);
}

.filter-row:last-child {
    border-bottom: none;
    padding-bottom: 0;
}

@media (max-width: 900px) {
    .filter-row {
        flex-wrap: wrap;
    }

    .filter-row .value-input {
        flex: 1;
        min-width: 100px;
    }

    .filter-row .btn-remove {
        order: 10;
        margin-left: auto;
    }
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
    justify-content: space-between;
    align-items: center;
    gap: 0.5rem;
}

.builder-actions-right {
    display: flex;
    gap: 0.5rem;
}
</style>
