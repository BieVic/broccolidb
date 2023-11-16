use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::{
    data_type::Int32Type, file::writer::SerializedFileWriter, schema::parser::parse_message_type,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::{fs, path::Path, sync::Arc};

fn main() {
    let sql = "CREATE TABLE table_1( \
           PersonID int, \
           FirstName varchar(255))";

    let dialect = GenericDialect {};

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    println!("AST: {:?}", ast);

    let path = Path::new("testfiles/simple.parquet");

    let message_type = "message schema {REQUIRED INT32 b;}";

    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let file = fs::File::create(&path).unwrap();
    let mut writer = SerializedFileWriter::new(&file, schema, Default::default()).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        col_writer
            .typed::<Int32Type>()
            .write_batch(&[1, 2, 3], Some(&[3, 3, 3]), Some(&[0, 1, 0]))
            .unwrap();
        col_writer.close().unwrap()
    }
    row_group_writer.close().unwrap();
    writer.close().unwrap();

    println!("Write done!");

    let file2 = fs::File::open("testfiles/simple.parquet").unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file2).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();

    let record_batch = reader.next().unwrap().unwrap();

    println!("Read {} records.", record_batch.num_rows());
}
