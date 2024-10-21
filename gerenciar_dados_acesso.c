#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_EVENT_TIME_LEN 64
#define MAX_EVENT_TYPE_LEN 32
#define MAX_USER_SESSION_LEN 256

#define ORIGINAL_FILE_NAME "access.bin"
#define INDEX_FILE_NAME "access.idx"

#define RECORDS_PER_INDEX 100000
#define RECORDS_PER_PAGE 10

typedef struct {
    char event_time[MAX_EVENT_TIME_LEN];
    char event_type[MAX_EVENT_TYPE_LEN];
    long long product_id;
    long long user_id;
    char user_session[MAX_USER_SESSION_LEN];
    long long seq_key;
    int ativo;
} AccessRecord;

typedef struct {
    long long seq_key;
    long long record_index;
} IndexRecord;

void initialize_file() {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        fp = fopen(ORIGINAL_FILE_NAME, "wb");
        if (fp == NULL) {
            perror("Erro ao criar o arquivo de dados");
            exit(EXIT_FAILURE);
        }
        fclose(fp);
    } else {
        fclose(fp);
    }
}

AccessRecord create_sample_access_record(const char *event_time, const char *event_type,
                                         long long product_id, long long user_id,
                                         const char *user_session) {
    AccessRecord record;
    strncpy(record.event_time, event_time, MAX_EVENT_TIME_LEN - 1);
    record.event_time[MAX_EVENT_TIME_LEN - 1] = '\0';
    strncpy(record.event_type, event_type, MAX_EVENT_TYPE_LEN - 1);
    record.event_type[MAX_EVENT_TYPE_LEN - 1] = '\0';
    record.product_id = product_id;
    record.user_id = user_id;
    strncpy(record.user_session, user_session, MAX_USER_SESSION_LEN - 1);
    record.user_session[MAX_USER_SESSION_LEN - 1] = '\0';
    record.seq_key = 0;
    record.ativo = 1;
    return record;
}

long long get_next_seq_key() {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        perror("Erro ao abrir o arquivo de dados para leitura do seq_key");
        exit(EXIT_FAILURE);
    }

    fseek(fp, 0, SEEK_END);
    long long file_size = ftell(fp);
    long long num_records = file_size / sizeof(AccessRecord);

    if (num_records == 0) {
        fclose(fp);
        return 1;
    }

    fseek(fp, -((long long)sizeof(AccessRecord)), SEEK_END);
    AccessRecord last_record;
    fread(&last_record, sizeof(AccessRecord), 1, fp);
    fclose(fp);

    return last_record.seq_key + 1;
}

int insert_record(AccessRecord *record) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "ab");
    if (fp == NULL) {
        perror("Erro ao abrir o arquivo de dados para inserção");
        return -1;
    }

    record->seq_key = get_next_seq_key();

    if (fwrite(record, sizeof(AccessRecord), 1, fp) != 1) {
        perror("Erro ao escrever o registro no arquivo de dados");
        fclose(fp);
        return -1;
    }

    fclose(fp);
    return 0;
}

void display_records_via_page(long long page) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        printf("Erro ao abrir o arquivo de dados.\n");
        return;
    }

    AccessRecord record;
    long long records_to_skip = (page - 1) * RECORDS_PER_PAGE;
    long long skipped = 0;
    long long records_displayed = 0;

    printf("\nExibindo registros da página %lld:\n", page);
    while (fread(&record, sizeof(AccessRecord), 1, fp) == 1) {
        if (record.ativo) {
            if (skipped < records_to_skip) {
                skipped++;
                continue;
            }

            printf("Registro %lld:\n", record.seq_key);
            printf("  Event Time: %s\n", record.event_time);
            printf("  Event Type: %s\n", record.event_type);
            printf("  Product ID: %lld\n", record.product_id);
            printf("  User ID: %lld\n", record.user_id);
            printf("  User Session: %s\n", record.user_session);
            printf("  Seq Key: %lld\n", record.seq_key);
            printf("  Ativo: %s\n\n", record.ativo ? "Sim" : "Não");

            records_displayed++;
            if (records_displayed >= RECORDS_PER_PAGE) {
                break;
            }
        }
    }

    if (records_displayed == 0) {
        printf("Nenhum registro encontrado nesta página.\n");
    }

    fclose(fp);
}

int create_partial_index(const char *data_file, const char *index_file, int records_per_index) {
    FILE *fp_data = fopen(data_file, "rb");
    if (fp_data == NULL) {
        perror("Erro ao abrir o arquivo de dados para criar o índice");
        return -1;
    }

    FILE *fp_index = fopen(index_file, "wb");
    if (fp_index == NULL) {
        perror("Erro ao criar o arquivo de índice");
        fclose(fp_data);
        return -1;
    }

    AccessRecord record;
    long long record_index = 0;
    int count = 0;

    while (fread(&record, sizeof(AccessRecord), 1, fp_data) == 1) {
        if (record.ativo) {
            if (count % records_per_index == 0) {
                IndexRecord idx_record;
                idx_record.seq_key = record.seq_key;
                idx_record.record_index = record_index;
                fwrite(&idx_record, sizeof(IndexRecord), 1, fp_index);
            }
            count++;
        }
        record_index++;
    }

    fclose(fp_data);
    fclose(fp_index);

    printf("Índice parcial criado com sucesso.\n");
    return 0;
}

int binary_search_index(const char *index_file, long long target_seq_key, IndexRecord *result) {
    FILE *fp_index = fopen(index_file, "rb");
    if (fp_index == NULL) {
        perror("Erro ao abrir o arquivo de índice para pesquisa");
        return -1;
    }

    fseek(fp_index, 0, SEEK_END);
    long long file_size = ftell(fp_index);
    long long num_records = file_size / sizeof(IndexRecord);
    rewind(fp_index);

    long long left = 0;
    long long right = num_records - 1;
    long long mid;
    IndexRecord mid_record;

    while (left <= right) {
        mid = left + (right - left) / 2;
        fseek(fp_index, mid * sizeof(IndexRecord), SEEK_SET);
        fread(&mid_record, sizeof(IndexRecord), 1, fp_index);

        if (mid_record.seq_key == target_seq_key) {
            *result = mid_record;
            fclose(fp_index);
            return mid;
        } else if (mid_record.seq_key < target_seq_key) {
            left = mid + 1;
        } else {
            if (mid == 0) break;
            right = mid - 1;
        }
    }

    if (right >= 0) {
        fseek(fp_index, right * sizeof(IndexRecord), SEEK_SET);
        fread(&mid_record, sizeof(IndexRecord), 1, fp_index);
        *result = mid_record;
        fclose(fp_index);
        return right;
    }

    fclose(fp_index);
    return -1;
}

void query_using_partial_index_with_pagination(long long target_seq_key, long long page) {
    IndexRecord idx_record;
    int idx = binary_search_index(INDEX_FILE_NAME, target_seq_key, &idx_record);

    if (idx == -1) {
        printf("Seq Key %lld não encontrado no índice.\n", target_seq_key);
        return;
    }

    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        printf("Erro ao abrir o arquivo de dados.\n");
        return;
    }

    fseek(fp, idx_record.record_index * sizeof(AccessRecord), SEEK_SET);
    AccessRecord current_record;
    long long records_to_skip = (page - 1) * RECORDS_PER_PAGE;
    long long skipped = 0;
    long long records_displayed = 0;

    printf("\nBuscando por Seq Key %lld usando o índice parcial e exibindo a página %lld...\n", target_seq_key, page);

    while (fread(&current_record, sizeof(AccessRecord), 1, fp) == 1) {
        if (current_record.ativo) {
            if (skipped < records_to_skip) {
                skipped++;
                continue;
            }

            printf("\nRegistro Encontrado:\n");
            printf("  Event Time: %s\n", current_record.event_time);
            printf("  Event Type: %s\n", current_record.event_type);
            printf("  Product ID: %lld\n", current_record.product_id);
            printf("  User ID: %lld\n", current_record.user_id);
            printf("  User Session: %s\n", current_record.user_session);
            printf("  Seq Key: %lld\n", current_record.seq_key);
            printf("  Ativo: %s\n", current_record.ativo ? "Sim" : "Não");

            records_displayed++;
            if (records_displayed >= RECORDS_PER_PAGE) {
                break;
            }
        }
    }

    if (records_displayed == 0) {
        printf("\nNenhum registro ativo encontrado na página %lld.\n", page);
    }

    fclose(fp);
}

void remove_record(long long target_seq_key) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "r+b");
    if (fp == NULL) {
        perror("Erro ao abrir o arquivo de dados para remoção");
        return;
    }

    AccessRecord record;

    while (fread(&record, sizeof(AccessRecord), 1, fp) == 1) {
        if (record.seq_key == target_seq_key && record.ativo) {
            record.ativo = 0;
            fseek(fp, -sizeof(AccessRecord), SEEK_CUR);
            fwrite(&record, sizeof(AccessRecord), 1, fp);
            printf("Registro com Seq Key %lld foi inativado.\n", target_seq_key);
            fclose(fp);
            return;
        }
    }

    printf("Registro com Seq Key %lld não encontrado ou já está inativo.\n", target_seq_key);
    fclose(fp);
}

void update_partial_index() {
    if (create_partial_index(ORIGINAL_FILE_NAME, INDEX_FILE_NAME, RECORDS_PER_INDEX) != 0) {
        printf("Erro ao atualizar o índice parcial.\n");
    }
}

int main() {
    initialize_file();
    AccessRecord records_to_insert[] = {
        create_sample_access_record("2024-04-21 10:00:00", "LOGIN", 101, 1001, "SESSION_A"),
        create_sample_access_record("2024-04-21 10:05:00", "VIEW_PRODUCT", 102, 1002, "SESSION_B"),
        create_sample_access_record("2024-04-21 10:10:00", "ADD_TO_CART", 103, 1003, "SESSION_C"),
        create_sample_access_record("2024-04-21 10:15:00", "PURCHASE", 104, 1004, "SESSION_D"),
        create_sample_access_record("2024-04-21 10:20:00", "LOGOUT", 105, 1005, "SESSION_E"),
        create_sample_access_record("2024-04-21 10:25:00", "LOGIN", 106, 1006, "SESSION_F")
    };
    int num_records = sizeof(records_to_insert) / sizeof(records_to_insert[0]);

    for (int i = 0; i < num_records; ++i) {
        if (insert_record(&records_to_insert[i]) == 0) {
            printf("Registro com Seq Key %lld inserido com sucesso.\n", records_to_insert[i].seq_key);
        } else {
            printf("Falha ao inserir o registro com Seq Key %lld.\n", records_to_insert[i].seq_key);
        }
    }

    update_partial_index();
    display_records_via_page(1);
    long long search_seq_key = 3;
    query_using_partial_index_with_pagination(search_seq_key, 1);
    remove_record(search_seq_key);
    update_partial_index();
    query_using_partial_index_with_pagination(search_seq_key, 1);
    display_records_via_page(1);

    return 0;
}
