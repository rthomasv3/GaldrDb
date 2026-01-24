<script setup>
import { ref, computed, watch } from "vue";

const props = defineProps({
    document: {
        type: Object,
        default: null
    },
    collectionName: {
        type: String,
        required: true
    }
});

const emit = defineEmits(["save", "cancel"]);

const jsonText = ref("");
const error = ref(null);
const saving = ref(false);

const isNewDocument = computed(() => !props.document);

const title = computed(() => {
    if (isNewDocument.value) {
        return "New Document";
    }
    return `Edit Document #${props.document.id}`;
});

watch(() => props.document, (doc) => {
    if (doc) {
        try {
            const parsed = JSON.parse(doc.json);
            jsonText.value = JSON.stringify(parsed, null, 2);
        } catch {
            jsonText.value = doc.json;
        }
    } else {
        jsonText.value = "{\n  \n}";
    }
    error.value = null;
}, { immediate: true });

function validateJson() {
    try {
        const parsed = JSON.parse(jsonText.value);
        if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
            return "Document must be a JSON object";
        }
        if (!isNewDocument.value && parsed.Id !== props.document.id) {
            return "Cannot change document Id";
        }
        return null;
    } catch (e) {
        return `Invalid JSON: ${e.message}`;
    }
}

async function handleSave() {
    const validationError = validateJson();
    if (validationError) {
        error.value = validationError;
        return;
    }

    saving.value = true;
    error.value = null;

    try {
        const minifiedJson = JSON.stringify(JSON.parse(jsonText.value));

        let result;
        if (isNewDocument.value) {
            result = await galdrInvoke("insertDocument", {
                request: {
                    collection: props.collectionName,
                    json: minifiedJson
                }
            });
        } else {
            result = await galdrInvoke("replaceDocument", {
                request: {
                    collection: props.collectionName,
                    id: props.document.id,
                    json: minifiedJson
                }
            });
        }

        if (result.success) {
            emit("save", result.id);
        } else {
            error.value = result.error || "Failed to save document";
        }
    } catch (e) {
        error.value = e.message || "Failed to save document";
    } finally {
        saving.value = false;
    }
}

function handleCancel() {
    emit("cancel");
}

function formatJson() {
    try {
        const parsed = JSON.parse(jsonText.value);
        jsonText.value = JSON.stringify(parsed, null, 2);
        error.value = null;
    } catch (e) {
        error.value = `Cannot format: ${e.message}`;
    }
}
</script>

<template>
    <div class="document-editor">
        <div class="editor-header">
            <div class="editor-title">{{ title }}</div>
            <div class="editor-actions">
                <button class="btn btn-secondary" @click="formatJson" :disabled="saving">
                    Format
                </button>
                <button class="btn btn-secondary" @click="handleCancel" :disabled="saving">
                    Cancel
                </button>
                <button class="btn btn-primary" @click="handleSave" :disabled="saving">
                    {{ saving ? "Saving..." : "Save" }}
                </button>
            </div>
        </div>

        <div v-if="error" class="error-message">
            {{ error }}
        </div>

        <div class="editor-content">
            <textarea
                v-model="jsonText"
                class="json-editor"
                spellcheck="false"
                :disabled="saving"
            ></textarea>
        </div>
    </div>
</template>

<style scoped>
.document-editor {
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

.editor-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.625rem 1rem;
    border-bottom: 1px solid var(--border-color);
    background-color: var(--bg-tertiary);
}

.editor-title {
    font-weight: 600;
    font-size: 0.8125rem;
    color: var(--text-primary);
}

.editor-actions {
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.btn {
    padding: 0.375rem 0.75rem;
    border: none;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.15s ease;
}

.btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
}

.btn-primary {
    background-color: var(--accent-color);
    color: white;
}

.btn-primary:hover:not(:disabled) {
    background-color: var(--accent-hover);
}

.btn-secondary {
    background-color: var(--bg-primary);
    color: var(--text-primary);
    border: 1px solid var(--border-color);
}

.btn-secondary:hover:not(:disabled) {
    background-color: var(--bg-hover);
}

.error-message {
    padding: 0.625rem 1rem;
    background-color: #dc2626;
    color: white;
    font-size: 0.8125rem;
}

.editor-content {
    flex: 1;
    display: flex;
    overflow: hidden;
}

.json-editor {
    flex: 1;
    resize: none;
    border: none;
    padding: 1rem;
    font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace;
    font-size: 0.8125rem;
    line-height: 1.6;
    color: var(--text-primary);
    background-color: var(--bg-primary);
    outline: none;
}

.json-editor:disabled {
    opacity: 0.7;
}
</style>
