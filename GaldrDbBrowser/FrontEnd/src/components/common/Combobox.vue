<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from "vue";

const props = defineProps({
    modelValue: {
        type: String,
        default: ""
    },
    options: {
        type: Array,
        default: () => []
    },
    placeholder: {
        type: String,
        default: ""
    }
});

const emit = defineEmits(["update:modelValue"]);

const inputRef = ref(null);
const containerRef = ref(null);
const isOpen = ref(false);
const inputValue = ref(props.modelValue);
const isFiltering = ref(false);
const dropdownStyle = ref({});

watch(() => props.modelValue, (val) => {
    inputValue.value = val;
});

const displayedOptions = computed(() => {
    if (!isFiltering.value || !inputValue.value) {
        return props.options;
    }
    const search = inputValue.value.toLowerCase();
    return props.options.filter(opt =>
        opt.toLowerCase().includes(search)
    );
});

function updateDropdownPosition() {
    if (!containerRef.value) {
        return;
    }
    const rect = containerRef.value.getBoundingClientRect();
    dropdownStyle.value = {
        position: "fixed",
        top: `${rect.bottom + 4}px`,
        left: `${rect.left}px`,
        width: `${rect.width}px`,
        zIndex: 1000
    };
}

function handleInput(e) {
    inputValue.value = e.target.value;
    emit("update:modelValue", e.target.value);
    isFiltering.value = true;
    isOpen.value = true;
    nextTick(updateDropdownPosition);
}

function openDropdown() {
    isFiltering.value = false;
    isOpen.value = true;
    nextTick(updateDropdownPosition);
}

function toggleDropdown() {
    if (isOpen.value) {
        isOpen.value = false;
    } else {
        openDropdown();
    }
}

function selectOption(option, e) {
    e.preventDefault();
    e.stopPropagation();
    inputValue.value = option;
    emit("update:modelValue", option);
    isOpen.value = false;
    isFiltering.value = false;
}

function handleClickOutside(e) {
    if (containerRef.value && !containerRef.value.contains(e.target)) {
        isOpen.value = false;
        isFiltering.value = false;
    }
}

function handleKeydown(e) {
    if (e.key === "Escape") {
        isOpen.value = false;
        isFiltering.value = false;
    } else if (e.key === "ArrowDown" && !isOpen.value) {
        openDropdown();
    }
}

onMounted(() => {
    document.addEventListener("mousedown", handleClickOutside);
});

onUnmounted(() => {
    document.removeEventListener("mousedown", handleClickOutside);
});
</script>

<template>
    <div ref="containerRef" class="combobox-container">
        <div class="combobox-input-wrapper">
            <input
                ref="inputRef"
                type="text"
                :value="inputValue"
                :placeholder="placeholder"
                class="combobox-input"
                @input="handleInput"
                @click="openDropdown"
                @keydown="handleKeydown"
            />
            <button
                type="button"
                class="combobox-toggle"
                @mousedown.prevent="toggleDropdown"
                tabindex="-1"
            >
                <svg width="12" height="12" viewBox="0 0 20 20" fill="currentColor">
                    <path fill-rule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clip-rule="evenodd" />
                </svg>
            </button>
        </div>
        <Teleport to="body">
            <div
                v-if="isOpen && displayedOptions.length > 0"
                class="combobox-dropdown"
                :style="dropdownStyle"
            >
                <button
                    v-for="option in displayedOptions"
                    :key="option"
                    type="button"
                    class="combobox-option"
                    :class="{ selected: option === inputValue }"
                    @mousedown.prevent="selectOption(option, $event)"
                >
                    {{ option }}
                </button>
            </div>
        </Teleport>
    </div>
</template>

<style scoped>
.combobox-container {
    position: relative;
    min-width: 100px;
}

.combobox-input-wrapper {
    display: flex;
    align-items: center;
    background-color: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: 0.375rem;
    transition: border-color 0.15s ease, box-shadow 0.15s ease;
}

.combobox-input-wrapper:focus-within {
    border-color: var(--accent-color);
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.15);
}

.combobox-input {
    flex: 1;
    padding: 0.5rem 0.75rem;
    border: none;
    background: transparent;
    color: var(--text-primary);
    font-size: 0.8125rem;
    outline: none;
    min-width: 0;
    border-radius: 0;
    box-shadow: none;
}

.combobox-input:focus {
    outline: none;
    border: none;
    box-shadow: none;
}

.combobox-input::placeholder {
    color: var(--text-muted);
}

.combobox-toggle {
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0 0.5rem;
    background: transparent;
    border: none;
    color: var(--text-muted);
    cursor: pointer;
    height: 100%;
}

.combobox-toggle:hover {
    color: var(--text-primary);
}
</style>

<style>
/* Global styles for teleported dropdown */
.combobox-dropdown {
    background-color: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 0.375rem;
    box-shadow: var(--shadow-lg);
    max-height: 200px;
    overflow-y: auto;
}

.combobox-option {
    display: block;
    width: 100%;
    padding: 0.5rem 0.75rem;
    text-align: left;
    background: transparent;
    border: none;
    color: var(--text-primary);
    font-size: 0.8125rem;
    cursor: pointer;
    transition: background-color 0.1s ease;
}

.combobox-option:hover {
    background-color: var(--bg-tertiary);
}

.combobox-option.selected {
    background-color: var(--accent-color);
    color: white;
}
</style>
