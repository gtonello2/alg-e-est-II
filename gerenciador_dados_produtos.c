#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_CATEGORY_CODE_LEN 64
#define MAX_BRAND_LEN 32
#define ORIGINAL_FILE_NAME "products.bin"
#define SORTED_FILE_NAME "products_temp_sorted.bin"
#define CHUNK_SIZE 1000
#define RECORDS_PER_PAGE 10

#define INDEX_FILE_NAME "products.idx"
#define RECORDS_PER_INDEX 100000  

typedef struct {
    long long head_index;
} Header;

typedef struct {
    long long product_id;
    long long category_id;
    char category_code[MAX_CATEGORY_CODE_LEN];
    char brand[MAX_BRAND_LEN];
    float price;
    int ativo;
    long long seq_key;
    long long elo;
} ProductRecord;

typedef struct {
    long long product_id;
    long long record_index;
} IndexRecord;


void initialize_file() {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        fp = fopen(ORIGINAL_FILE_NAME, "wb");
        Header header;
        header.head_index = -1;
        fwrite(&header, sizeof(Header), 1, fp);
        fclose(fp);
    } else {
        fclose(fp);
    }
}


ProductRecord create_sample_product(long long product_id, long long category_id, const char *category_code, const char *brand, float price, int ativo) {
    ProductRecord record;
    record.product_id = product_id;
    record.category_id = category_id;
    strncpy(record.category_code, category_code, MAX_CATEGORY_CODE_LEN - 1);
    record.category_code[MAX_CATEGORY_CODE_LEN - 1] = '\0';
    strncpy(record.brand, brand, MAX_BRAND_LEN - 1);
    record.brand[MAX_BRAND_LEN - 1] = '\0';
    record.price = price;
    record.ativo = ativo;
    record.seq_key = 0; 
    record.elo = -1;    
    return record;
}

long long find_immediately_lower_product_id(long long target_product_id) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        return -1;
    }
    long long previous_index = -1;
    Header header;
    fread(&header, sizeof(Header), 1, fp);
    if (header.head_index != -1) {
        long long current_index = header.head_index;
        ProductRecord current_record;
        while (current_index != -1) {
            fseek(fp, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
            fread(&current_record, sizeof(ProductRecord), 1, fp);
            if (current_record.product_id < target_product_id && current_record.ativo) {
                previous_index = current_index;
            } else {
                break;
            }
            current_index = current_record.elo;
        }
    }
    fclose(fp);
    return previous_index;
}

int insert_record(const ProductRecord *record) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb+");
    if (fp == NULL) {
        perror("Erro ao abrir o arquivo para insercao");
        return -1;
    }

    Header header;
    fread(&header, sizeof(Header), 1, fp);
    if (header.head_index == -1) {

        fseek(fp, sizeof(Header), SEEK_SET);
        ProductRecord new_record = *record;
        new_record.elo = -1;
        new_record.seq_key = 1;
        fwrite(&new_record, sizeof(ProductRecord), 1, fp);
        header.head_index = 0;
        fseek(fp, 0, SEEK_SET);
        fwrite(&header, sizeof(Header), 1, fp);
        fclose(fp);
        return 0;
    }


    long long lower_index = find_immediately_lower_product_id(record->product_id);
    if (lower_index == -1) {

        fseek(fp, 0, SEEK_END);
        long long new_record_index = (ftell(fp) - sizeof(Header)) / sizeof(ProductRecord);
        ProductRecord new_record = *record;
        new_record.elo = header.head_index;
        new_record.seq_key = new_record_index + 1;
        fwrite(&new_record, sizeof(ProductRecord), 1, fp);
        header.head_index = new_record_index;
        fseek(fp, 0, SEEK_SET);
        fwrite(&header, sizeof(Header), 1, fp);
        fclose(fp);
        return 0;
    }

    fseek(fp, sizeof(Header) + lower_index * sizeof(ProductRecord), SEEK_SET);
    ProductRecord low_record;
    fread(&low_record, sizeof(ProductRecord), 1, fp);


    fseek(fp, 0, SEEK_END);
    long long new_record_index = (ftell(fp) - sizeof(Header)) / sizeof(ProductRecord);
    ProductRecord new_record = *record;
    new_record.elo = low_record.elo;
    new_record.seq_key = new_record_index + 1;
    fwrite(&new_record, sizeof(ProductRecord), 1, fp);

    low_record.elo = new_record_index;
    fseek(fp, sizeof(Header) + lower_index * sizeof(ProductRecord), SEEK_SET);
    fwrite(&low_record, sizeof(ProductRecord), 1, fp);

    fclose(fp);
    return 0;
}

void remove_record(long long target_product_id) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "r+b");
    if (fp == NULL) {
        perror("Erro ao abrir o arquivo para remocao");
        return;
    }

    Header header;
    fread(&header, sizeof(Header), 1, fp);

    long long current_index = header.head_index;
    ProductRecord current_record;
    while (current_index != -1) {
        fseek(fp, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
        fread(&current_record, sizeof(ProductRecord), 1, fp);
        if (current_record.product_id == target_product_id && current_record.ativo) {

            current_record.ativo = 0;
            fseek(fp, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
            fwrite(&current_record, sizeof(ProductRecord), 1, fp);
            printf("Produto com product_id %lld foi removido (inativado).\n", target_product_id);
            fclose(fp);
            return;
        }
        current_index = current_record.elo;
    }

    printf("Produto com product_id %lld nao encontrado ou ja esta inativo.\n", target_product_id);
    fclose(fp);
}

int create_partial_index(const char *data_file, const char *index_file, int records_per_index) {
    FILE *fp_data = fopen(data_file, "rb");
    if (fp_data == NULL) {
        perror("Erro ao abrir o arquivo de dados para criar o indice");
        return -1;
    }

    FILE *fp_index = fopen(index_file, "wb");
    if (fp_index == NULL) {
        perror("Erro ao criar o arquivo de indice");
        fclose(fp_data);
        return -1;
    }

    Header header;
    fread(&header, sizeof(Header), 1, fp_data);

    long long current_index = header.head_index;
    ProductRecord current_record;
    int count = 0;

    while (current_index != -1) {
        fseek(fp_data, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
        fread(&current_record, sizeof(ProductRecord), 1, fp_data);

        if (current_record.ativo) {
            if (count % records_per_index == 0) {
                IndexRecord idx_record;
                idx_record.product_id = current_record.product_id;
                idx_record.record_index = current_index;
                fwrite(&idx_record, sizeof(IndexRecord), 1, fp_index);
            }
            count++;
        }

        current_index = current_record.elo;
    }

    fclose(fp_data);
    fclose(fp_index);

    printf("Indice parcial criado com sucesso.\n");
    return 0;
}


int binary_search_index(const char *index_file, long long target_product_id, IndexRecord *result) {
    FILE *fp_index = fopen(index_file, "rb");
    if (fp_index == NULL) {
        perror("Erro ao abrir o arquivo de indice para pesquisa");
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

        if (mid_record.product_id == target_product_id) {
            *result = mid_record;
            fclose(fp_index);
            return mid;
        } else if (mid_record.product_id < target_product_id) {
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

void query_using_partial_index(long long target_product_id) {
    IndexRecord idx_record;
    int idx = binary_search_index(INDEX_FILE_NAME, target_product_id, &idx_record);

    if (idx == -1) {
        printf("Produto com product_id %lld nao encontrado no indice.\n", target_product_id);
        return;
    }

    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        printf("Erro ao abrir o arquivo de dados.\n");
        return;
    }

    long long current_index = idx_record.record_index;
    ProductRecord current_record;

    while (current_index != -1) {
        fseek(fp, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
        fread(&current_record, sizeof(ProductRecord), 1, fp);

        if (current_record.product_id == target_product_id && current_record.ativo) {
            printf("\nProduto encontrado via indice parcial:\n");
            printf("  Product ID: %lld\n", current_record.product_id);
            printf("  Category ID: %lld\n", current_record.category_id);
            printf("  Category Code: %s\n", current_record.category_code);
            printf("  Brand: %s\n", current_record.brand);
            printf("  Price: %.2f\n", current_record.price);
            printf("  Ativo: %s\n", current_record.ativo ? "Sim" : "Nao");
            printf("  Seq Key: %lld\n", current_record.seq_key);
            fclose(fp);
            return;
        } else if (current_record.product_id > target_product_id) {
            break;
        }

        current_index = current_record.elo;
    }

    printf("\nProduto com product_id %lld nao encontrado.\n", target_product_id);
    fclose(fp);
}


int replace_original_with_sorted(const char *original_file, const char *sorted_file) {
    remove(original_file);
    rename(sorted_file, original_file);
    return 0;
}


void display_records_via_elo(long long pag) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        printf("Erro ao abrir o arquivo.\n");
        return;
    }

    Header header;
    fread(&header, sizeof(Header), 1, fp);
    if (header.head_index == -1) {
        printf("Nenhum registro encontrado.\n");
        fclose(fp);
        return;
    }

    long long current_index = header.head_index;
    ProductRecord current_record;

    long long records_to_skip = (pag - 1) * RECORDS_PER_PAGE;
    long long skipped_records = 0;

    while (skipped_records < records_to_skip && current_index != -1) {
        fseek(fp, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
        fread(&current_record, sizeof(ProductRecord), 1, fp);
        if (current_record.ativo) {
            skipped_records++;
        }
        current_index = current_record.elo;
    }

    if (current_index == -1 && skipped_records < records_to_skip) {
        printf("Pagina invalida ou sem registros suficientes.\n");
        fclose(fp);
        return;
    }

    printf("\nExibindo registros da pagina %lld seguindo os elos:\n", pag);
    long long records_displayed = 0;
    while (records_displayed < RECORDS_PER_PAGE && current_index != -1) {
        fseek(fp, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
        fread(&current_record, sizeof(ProductRecord), 1, fp);

        if (current_record.ativo) {
            printf("Registro %lld:\n", current_record.seq_key);
            printf("  Product ID: %lld\n", current_record.product_id);
            printf("  Category ID: %lld\n", current_record.category_id);
            printf("  Category Code: %s\n", current_record.category_code);
            printf("  Brand: %s\n", current_record.brand);
            printf("  Price: %.2f\n", current_record.price);
            printf("  Ativo: %s\n", current_record.ativo ? "Sim" : "Nao");
            printf("  Seq Key: %lld\n", current_record.seq_key);
            printf("  Elo (Proximo Indice): %lld\n\n", current_record.elo);

            records_displayed++;
        }

        current_index = current_record.elo;
    }

    if (records_displayed == 0) {
        printf("Nenhum registro ativo encontrado nesta pagina.\n");
    }

    fclose(fp);
}


void print_all_records_sequential(long long pag) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        printf("Erro ao abrir o arquivo.\n");
        return;
    }

    Header header;
    fread(&header, sizeof(Header), 1, fp);

    fseek(fp, 0, SEEK_END);
    long long file_size = ftell(fp);
    long long num_records = (file_size - sizeof(Header)) / sizeof(ProductRecord);

    if (pag < 1 || (pag - 1) * RECORDS_PER_PAGE >= num_records) {
        printf("Pagina invalida.\n");
        fclose(fp);
        return;
    }

    long long start_record = (pag - 1) * RECORDS_PER_PAGE;
    fseek(fp, sizeof(Header) + start_record * sizeof(ProductRecord), SEEK_SET);

    printf("\nExibindo registros da pagina %lld:\n", pag);
    ProductRecord record;
    for (long long i = 0; i < RECORDS_PER_PAGE && (start_record + i) < num_records; i++) {
        fread(&record, sizeof(ProductRecord), 1, fp);

        if (record.ativo) {
            printf("Registro %lld:\n", start_record + i + 1);
            printf("  Product ID: %lld\n", record.product_id);
            printf("  Category ID: %lld\n", record.category_id);
            printf("  Category Code: %s\n", record.category_code);
            printf("  Brand: %s\n", record.brand);
            printf("  Price: %.2f\n", record.price);
            printf("  Ativo: %s\n", record.ativo ? "Sim" : "Nao");
            printf("  Seq Key: %lld\n\n", record.seq_key);
        }
    }

    fclose(fp);
}


void search_and_display_product(long long target_product_id) {
    FILE *fp = fopen(ORIGINAL_FILE_NAME, "rb");
    if (fp == NULL) {
        printf("Erro ao abrir o arquivo de dados.\n");
        return;
    }

    Header header;
    fread(&header, sizeof(Header), 1, fp);

    long long current_index = header.head_index;
    ProductRecord current_record;

    while (current_index != -1) {
        fseek(fp, sizeof(Header) + current_index * sizeof(ProductRecord), SEEK_SET);
        fread(&current_record, sizeof(ProductRecord), 1, fp);

        if (current_record.product_id == target_product_id && current_record.ativo) {
            printf("\nProduto encontrado no indice %lld:\n", current_index + 1);
            printf("  Product ID: %lld\n", current_record.product_id);
            printf("  Category ID: %lld\n", current_record.category_id);
            printf("  Category Code: %s\n", current_record.category_code);
            printf("  Brand: %s\n", current_record.brand);
            printf("  Price: %.2f\n", current_record.price);
            printf("  Ativo: %s\n", current_record.ativo ? "Sim" : "Nao");
            printf("  Seq Key: %lld\n", current_record.seq_key);
            fclose(fp);
            return;
        }

        current_index = current_record.elo;
    }

    printf("\nProduto com product_id %lld nao encontrado.\n", target_product_id);
    fclose(fp);
}


void update_partial_index() {
    if (create_partial_index(ORIGINAL_FILE_NAME, INDEX_FILE_NAME, RECORDS_PER_INDEX) != 0) {
        printf("Erro ao atualizar o indice parcial.\n");
    }
}

int main() {
    initialize_file();
    printf("Inserindo registros de exemplo...\n");
    // ProductRecord records_to_insert[] = {
    //     create_sample_product(101, 1, "CAT01", "BrandA", 19.99, 1),
    //     create_sample_product(103, 2, "CAT02", "BrandB", 29.99, 1),
    //     create_sample_product(102, 1, "CAT01", "BrandC", 24.99, 1),
    //     create_sample_product(105, 3, "CAT03", "BrandA", 39.99, 1),
    //     create_sample_product(104, 2, "CAT02", "BrandD", 34.99, 1),
    //     create_sample_product(100, 1, "CAT00", "BrandE", 9.99, 1)
    // };
    // int num_records = sizeof(records_to_insert) / sizeof(records_to_insert[0]);
    // for (int i = 0; i < num_records; ++i) {
    //     if (insert_record(&records_to_insert[i]) == 0) {
    //         printf("Registro com product_id %lld inserido com sucesso.\n", records_to_insert[i].product_id);
    //     } else {
    //         printf("Falha ao inserir o registro com product_id %lld.\n", records_to_insert[i].product_id);
    //     }
    // }
    printf("Insercao de registros concluida.\n");

   
    update_partial_index();

    display_records_via_elo(1);

  
    long long search_id = 102;
    printf("\nRealizando busca pelo product_id %lld utilizando o indice parcial...\n", search_id);
    query_using_partial_index(search_id);


    print_all_records_sequential(1);


    printf("\nRemovendo o produto com product_id %lld...\n", search_id);
    remove_record(search_id);

    update_partial_index();


    printf("\nRealizando nova busca pelo product_id %lld apos remocao...\n", search_id);
    query_using_partial_index(search_id);

    return 0;
}
