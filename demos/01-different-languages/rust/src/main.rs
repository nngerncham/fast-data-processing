use csv::{QuoteStyle, Writer, WriterBuilder};
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

fn parse_raw(
    raw_file_directory: &Path,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let mut email_data = Vec::new();
    for entry in WalkDir::new(raw_file_directory)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let email_path = entry.path();
            let mut email_content = String::new();

            let email_fp = File::open(email_path)?;
            let mut rdr = DecodeReaderBytesBuilder::new()
                .encoding(Some(WINDOWS_1252))
                .build(email_fp);
            rdr.read_to_string(&mut email_content)?;

            email_data.push((email_path.to_string_lossy().into_owned(), email_content));
        }
    }

    Ok(email_data)
}

fn write_to_csv(
    parsed_data: &[(String, String)],
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = WriterBuilder::new()
        .quote_style(QuoteStyle::Always)
        .quote(b'|')
        .from_path(output_path)?;
    writer.write_record(["path", "content"])?;

    for (path, content) in parsed_data {
        writer.write_record([path, content])?;
    }
    writer.flush()?;

    Ok(())
}

fn parse_and_write_once(
    input_path: &Path,
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed_data = parse_raw(input_path)?;
    write_to_csv(&parsed_data, output_path)?;
    Ok(())
}

fn benchmark_once(
    run_idx: usize,
    input_path: &Path,
    output_path: &Path,
) -> Result<f64, Box<dyn std::error::Error>> {
    print!("Run number {} ", run_idx + 1);

    let start_time = Instant::now();
    parse_and_write_once(input_path, output_path)?;
    let run_time = start_time.elapsed().as_secs_f64();
    println!("took {}", run_time);

    Ok(run_time)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let n_runs = 100;
    let mut run_times = Vec::with_capacity(n_runs);
    let input_path = PathBuf::from("../../../dataset/enron-emails/unzipped_files/maildir/");
    let output_path = PathBuf::from("../../../outputs/01/rust_result.csv");

    for run_idx in 0..n_runs {
        let run_time = benchmark_once(run_idx, &input_path, &output_path)?;
        run_times.push(run_time);
    }

    let avg_run_times_all = run_times.iter().sum::<f64>() / run_times.len() as f64;
    let avg_run_times_warm_cache =
        run_times[1..].iter().sum::<f64>() / (run_times.len() - 1) as f64;

    let run_times_output_path = PathBuf::from("../../../outputs/01/rust_run_times.csv");
    let mut writer = Writer::from_path(run_times_output_path)?;
    writer.write_record(["run_id", "run_time_seconds"])?;
    for (run_idx, &run_time) in run_times.iter().enumerate() {
        writer.write_record(&[(run_idx + 1).to_string(), run_time.to_string()])?;
    }
    writer.flush()?;

    println!(
        "Avg: {}, warm cache run times: {}",
        avg_run_times_all, avg_run_times_warm_cache
    );

    Ok(())
}
