#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_EVENT_TIME_LEN 64
#define MAX_EVENT_TYPE_LEN 32
#define MAX_USER_SESSION_LEN 256
#define MAX_CATEGORY_CODE_LEN 64
#define MAX_BRAND_LEN 32

#define CHUNK_SIZE 131700

typedef struct {
    long long head_index;
} Header;


typedef struct {
    char event_time[MAX_EVENT_TIME_LEN];     // Hora do evento
    char event_type[MAX_EVENT_TYPE_LEN];     // Tipo do evento
    long long product_id;                    // ID do produto
    long long user_id;                       // ID do usuário
    char user_session[MAX_USER_SESSION_LEN]; // Sessão do usuário
    long long seq_key;                       // Chave sequencial                          // Elo, inicializado como 0
    int ativo;                              // Status ativo, inicializado como true
} AccessRecord;

typedef struct {
    long long product_id;                    // ID do produto (chave)
    long long category_id;                   // ID da categoria
    char category_code[MAX_CATEGORY_CODE_LEN]; // Código da categoria
    char brand[MAX_BRAND_LEN];               // Marca
    float price;                             // Preço
    int ativo;                              // Status ativo, inicializado como true
    long long seq_key;                       // Chave sequencial
    long long elo;                           // Elo, inicializado como 0
} ProductRecord;


// Protótipos das funções
void external_sort_access(const char *input_filename, const char *output_filename);
void external_sort_products(const char *input_filename, const char *output_filename);
void merge_files(const char *output_filename, char **temp_files, int num_temp_files, size_t record_size, int (*compare)(const void *, const void *), int eliminate_duplicates);
int compare_access_records(const void *a, const void *b);
int compare_product_records(const void *a, const void *b);
void pad_string(char *str, int size);
void quicksort(void *arr, int left, int right, size_t size, int (*compare)(const void *, const void *));
int partition(void *arr, int left, int right, size_t size, int (*compare)(const void *, const void *));
void swap_records(void *a, void *b, size_t size);

int main() {
    const char *input_filename = "dados.csv"; // Substitua pelo nome do seu arquivo
    // Processa e ordena os registros de acesso
    external_sort_access(input_filename, "access.bin");

    // Processa e ordena os registros de produtos
    external_sort_products(input_filename, "products.bin");

    return 0;
}

/**
 * Lê o arquivo de entrada, extrai registros de acesso, atribui uma chave sequencial
 * e grava diretamente no arquivo de saída.
 */
void external_sort_access(const char *input_filename, const char *output_filename) {
    // Abre o arquivo de entrada
    FILE *fp = fopen(input_filename, "r");
    if (!fp) {
        perror("Não foi possível abrir o arquivo de entrada");
        exit(EXIT_FAILURE);
    }

    // Abre o arquivo de saída
    FILE *output_fp = fopen(output_filename, "wb");
    if (!output_fp) {
        perror("Não foi possível abrir o arquivo de saída");
        exit(EXIT_FAILURE);
    }

    size_t access_capacity = CHUNK_SIZE;  // Máximo de registros por chunk
    AccessRecord *access_records = malloc(access_capacity * sizeof(AccessRecord));
    if (!access_records) {
        perror("Falha ao alocar memória para access_records");
        exit(EXIT_FAILURE);
    }

    char line[1024];
    long long seq_counter = 1;  // Contador de chave sequencial

    // Pula a linha de cabeçalho, se presente
    if (fgets(line, sizeof(line), fp)) {
        if (strstr(line, "event_time") == NULL) {
            // A primeira linha não é um cabeçalho
            fseek(fp, 0, SEEK_SET);
        }
    }

    while (1) {
        size_t access_count = 0;  // Número de registros lidos no chunk atual

        // Lê um chunk de dados
        while (access_count < access_capacity && fgets(line, sizeof(line), fp)) {
            char *p = line;
            int field = 0;
            char *token;

            char event_time[MAX_EVENT_TIME_LEN];
            char event_type[MAX_EVENT_TYPE_LEN];
            long long product_id = 0;  // Inicializa com valores padrão
            long long user_id = 0;
            char user_session[MAX_USER_SESSION_LEN];

            // Inicializa strings
            memset(event_time, 0, sizeof(event_time));
            memset(event_type, 0, sizeof(event_type));
            memset(user_session, 0, sizeof(user_session));

            while (field <= 8) {
                token = p;
                // Encontra a próxima vírgula ou fim de linha
                while (*p && *p != ',') p++;
                if (*p == ',') {
                    *p = '\0';  // Termina o campo atual
                    p++;        // Move para o próximo caractere
                } else if (*p == '\n' || *p == '\0') {
                    *p = '\0';  // Termina no fim da linha
                    p = NULL;   // Não há mais dados
                }

                // Agora token aponta para o campo atual
                switch (field) {
                    case 0:
                        strncpy(event_time, token, MAX_EVENT_TIME_LEN - 1);
                        pad_string(event_time, MAX_EVENT_TIME_LEN - 1);
                        break;
                    case 1:
                        strncpy(event_type, token, MAX_EVENT_TYPE_LEN - 1);
                        pad_string(event_type, MAX_EVENT_TYPE_LEN - 1);
                        break;
                    case 2:
                        if (*token != '\0') {
                            product_id = atoll(token);
                        }
                        break;
                    case 7:
                        if (*token != '\0') {
                            user_id = atoll(token);
                        }
                        break;
                    case 8:
                        strncpy(user_session, token, MAX_USER_SESSION_LEN - 1);
                        user_session[strcspn(user_session, "\n")] = '\0';
                        pad_string(user_session, MAX_USER_SESSION_LEN - 1);
                        break;
                    default:
                        break;
                }
                if (p == NULL) {
                    break;  // Fim da linha alcançado
                }
                field++;
            }

            // Atribui chave sequencial e adiciona aos registros de acesso
            AccessRecord *access_rec = &access_records[access_count++];
            strcpy(access_rec->event_time, event_time);
            strcpy(access_rec->event_type, event_type);
            access_rec->product_id = product_id;
            access_rec->user_id = user_id;
            strcpy(access_rec->user_session, user_session);
            access_rec->seq_key = seq_counter++;  // Atribui chave sequencial
            access_rec->ativo = 1;
        }

        if (access_count == 0) {
            break;  // Não há mais dados
        }

        // Escreve o chunk diretamente no arquivo de saída
        size_t write_count = fwrite(access_records, sizeof(AccessRecord), access_count, output_fp);
        if (write_count != access_count) {
            perror("Falha ao escrever todos os registros de acesso no arquivo de saída");
            exit(EXIT_FAILURE);
        }
    }

    free(access_records);
    fclose(fp);
    fclose(output_fp);
}

/**
 * Lê o arquivo de entrada, extrai registros de produtos, assegura que não haja IDs de produtos duplicados,
 * ordena cada chunk usando Quick Sort e mescla os chunks ordenados.
 */
void external_sort_products(const char *input_filename, const char *output_filename) {
    FILE *fp = fopen(input_filename, "r");
    if (!fp) {
        perror("Não foi possível abrir o arquivo de entrada");
        exit(EXIT_FAILURE);
    }

    char **temp_files = NULL;          // Array para armazenar nomes de arquivos temporários
    int temp_file_count = 0;           // Número de arquivos temporários criados
    size_t product_capacity = CHUNK_SIZE;
    ProductRecord *product_records = malloc(product_capacity * sizeof(ProductRecord));
    if (!product_records) {
        perror("Falha ao alocar memória para product_records");
        exit(EXIT_FAILURE);
    }

    char line[1024];

    // Pula a linha de cabeçalho, se presente
    if (fgets(line, sizeof(line), fp)) {
        if (strstr(line, "event_time") == NULL) {
            // A primeira linha não é um cabeçalho
            fseek(fp, 0, SEEK_SET);
        }
    }


    while (1) {
        size_t product_count = 0;  // Número de registros lidos no chunk atual

        // Lê um chunk de dados
        while (product_count < product_capacity && fgets(line, sizeof(line), fp)) {
            char *p = line;
            // Tokeniza a linha
            char *token;
            int field = 0;

            // Inicializa variáveis
            long long product_id = 0;
            long long category_id = 0;
            char category_code[MAX_CATEGORY_CODE_LEN];
            char brand[MAX_BRAND_LEN];
            float price = 0.0;

            // Inicializa strings
            memset(category_code, 0, sizeof(category_code));
            memset(brand, 0, sizeof(brand));

            while (field <= 6) {
                token = p;
                // Encontra a próxima vírgula ou fim de linha
                while (*p && *p != ',') p++;
                if (*p == ',') {
                    *p = '\0';  // Termina o campo atual
                    p++;        // Move para o próximo caractere
                } else if (*p == '\n' || *p == '\0') {
                    *p = '\0';  // Termina no fim da linha
                    p = NULL;   // Não há mais dados
                }

                switch (field) {
                    case 2:
                        if (*token != '\0') {
                            product_id = atoll(token);
                        }
                        break;
                    case 3:
                        if (*token != '\0') {
                            category_id = atoll(token);
                        }
                        break;
                    case 4:
                        strncpy(category_code, token, MAX_CATEGORY_CODE_LEN - 1);
                        pad_string(category_code, MAX_CATEGORY_CODE_LEN - 1);
                        break;
                    case 5:
                        strncpy(brand, token, MAX_BRAND_LEN - 1);
                        pad_string(brand, MAX_BRAND_LEN - 1);
                        break;
                    case 6:
                        if (*token != '\0') {
                            price = atof(token);
                        }
                        break;
                    default:
                        break;
                }
                field++;
                if (p == NULL) {
                    break;  // Fim da linha alcançado
                }
            }

            // Adiciona aos registros de produtos
            ProductRecord *product_rec = &product_records[product_count++];
            product_rec->product_id = product_id;
            product_rec->category_id = category_id;
            strcpy(product_rec->category_code, category_code);
            strcpy(product_rec->brand, brand);
            product_rec->price = price;
            product_rec->seq_key = 0; 
            product_rec->ativo = 1;
            product_rec->elo = 0;
        }

        if (product_count == 0) {
            break;  // Não há mais dados
        }

        // Ordena o chunk usando Quick Sort
        quicksort(product_records, 0, product_count - 1, sizeof(ProductRecord), compare_product_records);

        // Escreve o chunk ordenado em um arquivo temporário
        char temp_filename[30];
        sprintf(temp_filename, "product_temp_%d.bin", temp_file_count++);
        FILE *temp_fp = fopen(temp_filename, "wb");
        if (!temp_fp) {
            perror("Não foi possível abrir o arquivo temporário");
            exit(EXIT_FAILURE);
        }

        // Escreve cada registro no arquivo temporário
        size_t write_count = fwrite(product_records, sizeof(ProductRecord), product_count, temp_fp);
        if (write_count != product_count) {
            perror("Falha ao escrever todos os registros de produtos no arquivo temporário");
            exit(EXIT_FAILURE);
        }

        fclose(temp_fp);

        // Mantém o controle dos arquivos temporários
        char *temp_filename_dup = strdup(temp_filename);
        if (!temp_filename_dup) {
            perror("Falha ao duplicar o nome do arquivo temporário");
            exit(EXIT_FAILURE);
        }
        char **new_temp_files = realloc(temp_files, temp_file_count * sizeof(char *));
        if (!new_temp_files) {
            perror("Falha ao realocar memória para temp_files");
            exit(EXIT_FAILURE);
        }
        temp_files = new_temp_files;
        temp_files[temp_file_count - 1] = temp_filename_dup;
    }

    free(product_records);
    fclose(fp);

    // Mescla os arquivos temporários, eliminando IDs de produtos duplicados
    merge_files(output_filename, temp_files, temp_file_count, sizeof(ProductRecord), compare_product_records, 1);

    // Limpa os arquivos temporários
    for (int i = 0; i < temp_file_count; i++) {
        remove(temp_files[i]);
        free(temp_files[i]);
    }
    free(temp_files);
}

/**
 * Mescla arquivos temporários ordenados no arquivo de saída final.
 * Se eliminate_duplicates estiver definido, registros duplicados (baseados na chave) serão ignorados.
 */
void merge_files(const char *output_filename, char **temp_files, int num_temp_files, size_t record_size, int (*compare)(const void *, const void *), int eliminate_duplicates) {
    FILE **fps = malloc(num_temp_files * sizeof(FILE *));
    if (!fps) {
        perror("Falha ao alocar memória para ponteiros de arquivos");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < num_temp_files; i++) {
        fps[i] = fopen(temp_files[i], "rb");
        if (!fps[i]) {
            perror("Não foi possível abrir o arquivo temporário para mesclagem");
            exit(EXIT_FAILURE);
        }
    }

    ProductRecord *record = malloc(sizeof(ProductRecord));  // Aloca um ProductRecord para trabalhar diretamente
    if (!record) {
        perror("Falha ao alocar memória para ProductRecord");
        exit(EXIT_FAILURE);
    }

    void **buffers = malloc(num_temp_files * sizeof(void *));
    if (!buffers) {
        perror("Falha ao alocar memória para buffers");
        exit(EXIT_FAILURE);
    }
    int *active = malloc(num_temp_files * sizeof(int));
    if (!active) {
        perror("Falha ao alocar memória para o array active");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < num_temp_files; i++) {
        buffers[i] = malloc(record_size);
        if (!buffers[i]) {
            perror("Falha ao alocar memória para o buffer");
            exit(EXIT_FAILURE);
        }
        if (fread(buffers[i], record_size, 1, fps[i]) == 1) {
            active[i] = 1;
        } else {
            active[i] = 0;
        }
    }

    FILE *output_fp = fopen(output_filename, "wb+");
    if (!output_fp) {
        perror("Não foi possível abrir o arquivo de saída para mesclagem");
        exit(EXIT_FAILURE);
    }

    void *last_written_record = malloc(record_size);
    if (!last_written_record) {
        perror("Falha ao alocar memória para last_written_record");
        exit(EXIT_FAILURE);
    }

    Header header;
    header.head_index = 0;
    fwrite(&header, sizeof(Header), 1, output_fp); 
    int first_record = 1;
    long long seq_counter = 1;
    long last_written_pos = -1;

    while (1) {
        int min_index = -1;
        for (int i = 0; i < num_temp_files; i++) {
            if (active[i]) {
                if (min_index == -1) {
                    min_index = i;
                } else {
                    if (compare(buffers[i], buffers[min_index]) < 0) {
                        min_index = i;
                    }
                }
            }
        }

        if (min_index == -1) {
            break;
        }

        // Copia os dados do buffer para o ProductRecord diretamente
        memcpy(record, buffers[min_index], sizeof(ProductRecord));

        // Elimina duplicatas se necessário
        if (eliminate_duplicates) {
            if (first_record || compare(buffers[min_index], last_written_record) != 0) {
                record->elo = seq_counter;
                record->seq_key = seq_counter;

                // Escreve o ProductRecord diretamente no arquivo
                last_written_pos = ftell(output_fp);
                fwrite(record, sizeof(ProductRecord), 1, output_fp);
                memcpy(last_written_record, record, sizeof(ProductRecord));
                first_record = 0;
                seq_counter++;
            }
        } else {
            // Atualiza os contadores de sequência sem eliminar duplicatas
            record->elo = seq_counter;
            record->seq_key = seq_counter;

            last_written_pos = ftell(output_fp);
            fwrite(record, sizeof(ProductRecord), 1, output_fp);
            seq_counter++;
        }

        // Lê o próximo registro do arquivo temporário
        if (fread(buffers[min_index], record_size, 1, fps[min_index]) == 1) {
            active[min_index] = 1;
        } else {
            active[min_index] = 0;
        }
    }

    // Atualiza o último registro gravado, se necessário
    if (last_written_pos != -1) {
        fseek(output_fp, last_written_pos, SEEK_SET);
        fread(last_written_record, record_size, 1, output_fp);
        ProductRecord *last_record = (ProductRecord *)last_written_record;
        last_record->elo = -1;  // Marca como o último registro
        fseek(output_fp, last_written_pos, SEEK_SET);
        fwrite(last_record, sizeof(ProductRecord), 1, output_fp);
    }

    // Limpeza
    fclose(output_fp);
    free(last_written_record);
    free(record);
    for (int i = 0; i < num_temp_files; i++) {
        fclose(fps[i]);
        free(buffers[i]);
    }
    free(fps);
    free(buffers);
    free(active);
}



/**
 * Compara duas estruturas ProductRecord com base em product_id.
 */
int compare_product_records(const void *a, const void *b) {
    const ProductRecord *recA = (const ProductRecord *)a;
    const ProductRecord *recB = (const ProductRecord *)b;

    if (recA->product_id < recB->product_id)
        return -1;
    else if (recA->product_id > recB->product_id)
        return 1;
    else
        return 0;
}

/**
 * Preenche uma string com espaços para garantir que tenha tamanho fixo.
 */
void pad_string(char *str, int size) {
    int len = strlen(str);
    for (int i = len; i < size; i++) {
        str[i] = ' ';
    }
    str[size] = '\0';
}

/**
 * Implementação do Quick Sort para ordenar um array.
 * Esta função ordena o array do índice 'left' ao 'right'.
 */
void quicksort(void *arr, int left, int right, size_t size, int (*compare)(const void *, const void *)) {
    if (left < right) {
        // Particiona o array e obtém o índice do pivô
        int pivot_index = partition(arr, left, right, size, compare);

        // Ordena recursivamente os elementos antes e depois da partição
        quicksort(arr, left, pivot_index - 1, size, compare);
        quicksort(arr, pivot_index + 1, right, size, compare);
    }
}

/**
 * Particiona o array e retorna o índice do pivô.
 */
int partition(void *arr, int left, int right, size_t size, int (*compare)(const void *, const void *)) {
    char *array = (char *)arr;
    void *pivot = array + right * size;  // Escolhe o último elemento como pivô
    int i = left - 1;

    for (int j = left; j < right; j++) {
        if (compare(array + j * size, pivot) <= 0) {
            i++;
            swap_records(array + i * size, array + j * size, size);
        }
    }
    swap_records(array + (i + 1) * size, array + right * size, size);
    return i + 1;
}

/**
 * Troca dois registros de tamanho 'size'.
 */
void swap_records(void *a, void *b, size_t size) {
    // Usa um buffer estático para evitar malloc/free frequentes
    char temp[sizeof(ProductRecord) > sizeof(AccessRecord) ? sizeof(ProductRecord) : sizeof(AccessRecord)];

    memcpy(temp, a, size);
    memcpy(a, b, size);
    memcpy(b, temp, size);
}