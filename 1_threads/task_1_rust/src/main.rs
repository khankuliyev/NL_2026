//! Параллельный поиск файлов-дубликатов.
//!
//! 1. Рекурсивный обход каталога, группировка файлов по размеру.
//! 2. Файлы с неуникальным размером раздаются рабочим потокам через канал mpsc,
//!    каждый поток считает хеш содержимого и отправляет результат главному потоку.
//! 3. Файлы с совпавшими (размер, хеш) сравниваются побайтово.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::hash::Hasher;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const BUF_SIZE: usize = 64 * 1024;

/// Рекурсивно обходит каталог и собирает пары (путь, размер).
/// Символические ссылки пропускаются, пустые файлы тоже.
fn walk(dir: &Path, out: &mut Vec<(PathBuf, u64)>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("не удалось прочитать {}: {}", dir.display(), e);
            return;
        }
    };
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        let path = entry.path();
        if ft.is_dir() {
            walk(&path, out);
        } else if ft.is_file() {
            if let Ok(meta) = entry.metadata() {
                if meta.len() > 0 {
                    out.push((path, meta.len()));
                }
            }
        }
    }
}

/// Хеш содержимого файла, читается блоками.
fn hash_file(path: &Path) -> io::Result<u64> {
    let mut reader = BufReader::with_capacity(BUF_SIZE, File::open(path)?);
    let mut hasher = DefaultHasher::new();
    let mut buf = vec![0u8; BUF_SIZE];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.write(&buf[..n]);
    }
    Ok(hasher.finish())
}

/// Побайтовое сравнение двух файлов одинакового размера.
fn same_content(a: &Path, b: &Path) -> io::Result<bool> {
    let mut ra = BufReader::with_capacity(BUF_SIZE, File::open(a)?);
    let mut rb = BufReader::with_capacity(BUF_SIZE, File::open(b)?);
    let mut ba = vec![0u8; BUF_SIZE];
    let mut bb = vec![0u8; BUF_SIZE];
    loop {
        let na = read_full(&mut ra, &mut ba)?;
        let nb = read_full(&mut rb, &mut bb)?;
        if na != nb || ba[..na] != bb[..nb] {
            return Ok(false);
        }
        if na == 0 {
            return Ok(true);
        }
    }
}

/// Читает, пока буфер не заполнится или не кончится файл.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let n = r.read(&mut buf[total..])?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}

/// Считает хеши файлов на `threads` рабочих потоках.
/// Задания идут к потокам через канал mpsc (приёмник общий, под мьютексом),
/// результаты возвращаются главному потоку через второй канал.
fn hash_parallel(files: &[(PathBuf, u64)], threads: usize) -> Vec<Option<u64>> {
    let (job_tx, job_rx) = mpsc::channel::<(usize, PathBuf)>();
    let (res_tx, res_rx) = mpsc::channel::<(usize, io::Result<u64>)>();
    let job_rx = Arc::new(Mutex::new(job_rx));

    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let job_rx = Arc::clone(&job_rx);
            let res_tx = res_tx.clone();
            thread::spawn(move || loop {
                // Блокировка держится только на время получения задания.
                let job = job_rx.lock().unwrap().recv();
                let Ok((idx, path)) = job else { break };
                let h = hash_file(&path);
                if res_tx.send((idx, h)).is_err() {
                    break;
                }
            })
        })
        .collect();
    drop(res_tx);

    for (i, (path, _)) in files.iter().enumerate() {
        job_tx.send((i, path.clone())).unwrap();
    }
    drop(job_tx); // рабочие завершатся, когда очередь опустеет

    let mut hashes = vec![None; files.len()];
    for (idx, h) in res_rx {
        match h {
            Ok(h) => hashes[idx] = Some(h),
            Err(e) => eprintln!("ошибка чтения {}: {}", files[idx].0.display(), e),
        }
    }
    for w in workers {
        w.join().unwrap();
    }
    hashes
}

/// Группа одинаковых файлов.
struct DupGroup {
    size: u64,
    paths: Vec<PathBuf>,
}

/// Полный поиск дубликатов среди кандидатов (файлов с неуникальным размером).
fn find_duplicates(candidates: &[(PathBuf, u64)], threads: usize) -> Vec<DupGroup> {
    let hashes = hash_parallel(candidates, threads);

    let mut by_hash: HashMap<(u64, u64), Vec<usize>> = HashMap::new();
    for (i, h) in hashes.iter().enumerate() {
        if let Some(h) = h {
            by_hash.entry((candidates[i].1, *h)).or_default().push(i);
        }
    }

    let mut groups = Vec::new();
    for ((size, _), idxs) in by_hash {
        if idxs.len() < 2 {
            continue;
        }
        // Совпадение хеша ещё не гарантирует равенства: разбиваем на классы
        // побайтовым сравнением с первым файлом каждого класса.
        let mut classes: Vec<Vec<usize>> = Vec::new();
        for i in idxs {
            let path = &candidates[i].0;
            let found = classes.iter_mut().find(|c| {
                same_content(&candidates[c[0]].0, path).unwrap_or(false)
            });
            match found {
                Some(c) => c.push(i),
                None => classes.push(vec![i]),
            }
        }
        for c in classes.into_iter().filter(|c| c.len() > 1) {
            let mut paths: Vec<PathBuf> = c.iter().map(|&i| candidates[i].0.clone()).collect();
            paths.sort();
            groups.push(DupGroup { size, paths });
        }
    }
    groups.sort_by(|a, b| {
        let wa = a.size * (a.paths.len() as u64 - 1);
        let wb = b.size * (b.paths.len() as u64 - 1);
        wb.cmp(&wa).then_with(|| a.paths.cmp(&b.paths))
    });
    groups
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["Б", "КБ", "МБ", "ГБ", "ТБ"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{} {}", bytes, UNITS[0])
    } else {
        format!("{:.2} {}", v, UNITS[u])
    }
}

fn timed<T>(f: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let r = f();
    (r, start.elapsed())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        eprintln!("использование: {} <каталог> [число потоков]", args[0]);
        process::exit(1);
    }
    let root = PathBuf::from(&args[1]);
    if !root.is_dir() {
        eprintln!("{} не является каталогом", root.display());
        process::exit(1);
    }
    let threads = match args.get(2) {
        Some(s) => match s.parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                eprintln!("число потоков должно быть положительным целым");
                process::exit(1);
            }
        },
        None => thread::available_parallelism().map(|n| n.get()).unwrap_or(4),
    };

    // Обход каталога и группировка по размеру.
    let (all_files, t_walk) = timed(|| {
        let mut v = Vec::new();
        walk(&root, &mut v);
        v
    });
    let mut by_size: HashMap<u64, usize> = HashMap::new();
    for (_, size) in &all_files {
        *by_size.entry(*size).or_default() += 1;
    }
    let candidates: Vec<(PathBuf, u64)> = all_files
        .iter()
        .filter(|(_, size)| by_size[size] > 1)
        .cloned()
        .collect();

    println!("Каталог: {}", root.display());
    println!("Найдено непустых файлов: {} (обход за {:.3} с)", all_files.len(), t_walk.as_secs_f64());
    println!("Кандидатов (размер не уникален): {}", candidates.len());

    // Прогрев: первый проход подтягивает файлы в кеш ОС, чтобы сравнение
    // 1 и N потоков шло в одинаковых условиях.
    find_duplicates(&candidates, threads);

    let (groups, t1) = timed(|| find_duplicates(&candidates, 1));
    let (groups_n, tn) = timed(|| find_duplicates(&candidates, threads));
    assert_eq!(groups.len(), groups_n.len(), "результаты на 1 и N потоках разошлись");

    println!();
    if groups.is_empty() {
        println!("Дубликаты не найдены.");
    }
    let mut wasted = 0u64;
    let mut extra = 0usize;
    for (i, g) in groups.iter().enumerate() {
        let copies = g.paths.len() as u64 - 1;
        wasted += g.size * copies;
        extra += copies as usize;
        println!("Группа {}: {} файла(ов) по {} ({} байт)", i + 1, g.paths.len(), human(g.size), g.size);
        for p in &g.paths {
            println!("    {}", p.display());
        }
    }

    println!();
    println!("Групп дубликатов: {}", groups.len());
    println!("Лишних копий: {}, занимают {} ({} байт)", extra, human(wasted), wasted);
    println!();
    println!("Время (хеширование + сравнение, файлы в кеше ОС):");
    println!("    1 поток:   {:>9.3} мс", t1.as_secs_f64() * 1000.0);
    println!(
        "    {} потоков: {:>9.3} мс   ускорение x{:.2}",
        threads,
        tn.as_secs_f64() * 1000.0,
        t1.as_secs_f64() / tn.as_secs_f64()
    );
}
