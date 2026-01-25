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
            <button class="close-btn" @click="handleCancel" :disabled="saving">&times;</button>
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

        <div class="editor-footer">
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
</template>

<style scoped>
.document-editor {
    flex: 1;
    display: flex;
    flex-direction: column;
    background-color: var(--bg-secondary);
    border-left: 1px solid var(--border-color);
    overflow: hidden;
}

.editor-header {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    padding: 0 1rem;
    height: 3.5rem;
    border-bottom: 1px solid var(--border-color);
    background-color: var(--bg-secondary);
}

.editor-title {
    font-weight: 600;
    font-size: 0.875rem;
    color: var(--text-primary);
    flex: 1;
}

.close-btn {
    background: transparent;
    border: none;
    color: var(--text-muted);
    font-size: 1.5rem;
    line-height: 1;
    cursor: pointer;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
    transition: all 0.15s ease;
}

.close-btn:hover {
    color: var(--text-primary);
    background-color: var(--bg-hover);
}

.close-btn:disabled {
    opacity: 0.4;
    cursor: not-allowed;
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

.editor-footer {
    display: flex;
    justify-content: flex-end;
    align-items: center;
    gap: 0.5rem;
    padding: 0.625rem 1rem;
    border-top: 1px solid var(--border-color);
    background-color: var(--bg-secondary);
}
</style>
