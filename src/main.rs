use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::cmp::min;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
#[tokio::main]
async fn download_data(url_path: &str) {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("tmp_file");
    let mut file = File::create("data.csv")
        .await
        .expect("error on creating tmp file");
    let url = url_path;
    let response = reqwest::get(url);
    let mut stream = response.await.expect("error on streaming").bytes_stream();
    let total_size = reqwest::get(url)
        .await
        .expect("asdfd")
        .content_length()
        .ok_or(format!("Failed to get content length from '{}'", &url))
        .unwrap();
    let mut downloaded: u64 = 0;

    // Indicatif setup
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} {spinner:.yellow} {bytes}/{total_bytes} ETA: {eta}")
            .unwrap(),
    );
    pb.set_message(format!("Fetching Data..."));
    while let Some(item) = stream.next().await {
        let chunk = item.expect("Error while downloading file");
        file.write_all(&chunk)
            .await
            .expect("error on writing to file");
        let new = min(downloaded + (chunk.len() as u64), total_size);
        downloaded = new;
        pb.set_position(new);
    }
    pb.finish_with_message(format!("🐶 Success!"));
}

fn main() {
    download_data("https://github.com/Schlumberger/hackathon/blob/master/backend/dataset/data-large.csv?raw=true");
}
