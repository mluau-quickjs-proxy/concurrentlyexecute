use concurrentlyexec::ConcurrentExecutor;
use concurrentlyexec::ConcurrentExecutorState;
use concurrentlyexec::ConcurrentlyExecute;
use concurrentlyexec::ProcessOpts;
use serde::Deserialize;
use serde::Serialize;
use tokio::runtime::LocalOptions;
use tokio::runtime::Builder;

#[derive(Serialize, Deserialize)]
enum MpTaskMessage {
    Request {
        msg: String, // Some string from the server
        resp: concurrentlyexec::OneshotSender<String>, // A oneshot channel to send the response
    }
}

// A really dumb task
#[derive(Clone)]
struct MpTask {}

impl ConcurrentlyExecute for MpTask {
    type Message = MpTaskMessage;

    async fn run(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<concurrentlyexec::Message<Self>>, 
    ) {
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        concurrentlyexec::Message::Shutdown => {
                            break;
                        }
                        concurrentlyexec::Message::Data { data: MpTaskMessage::Request { msg, resp } } => {
                            // Just echo the message back
                            if msg == "0" {
                                // Simulate a long task
                                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                                let _ = resp.send("Long task done".to_string());
                                continue;
                            }
                            let _ = resp.send(format!("Echo: {}", msg));
                        }
                    }
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(120)) => {
                    panic!("Task should have been killed by now");
                }
            }
        }    
    }
}

async fn host() {
    let state = ConcurrentExecutorState::new(1);
    let executor = ConcurrentExecutor::<MpTask>::new_process(state, ProcessOpts::new(vec!["-".to_string(), "--child".to_string()])).await.unwrap();
    let (tx, rx) = executor.create_oneshot();
    let time = std::time::Instant::now();
    executor.send(MpTaskMessage::Request { msg: "Hello from host".to_string(), resp: tx }).unwrap();
    println!("Time taken [send]: {:?}", time.elapsed());
    let response = rx.recv().await.unwrap();
    println!("Time taken: {:?}", time.elapsed());
    println!("Got response from child: {}", response);

    let time = std::time::Instant::now();
    let (tx, rx) = executor.create_oneshot();
    executor.send(MpTaskMessage::Request { msg: "Hello from host 2".to_string(), resp: tx }).unwrap();
    println!("Time taken [send]: {:?}", time.elapsed());
    let response = rx.recv().await.unwrap();
    println!("Time taken: {:?}", time.elapsed());
    println!("Got response from child: {}", response);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let time = std::time::Instant::now();
    let (tx, rx) = executor.create_oneshot();
    executor.send(MpTaskMessage::Request { msg: "0".to_string(), resp: tx }).unwrap();
    println!("Time taken [send]: {:?}", time.elapsed());
    let response = rx.recv().await.unwrap();
    println!("Time taken: {:?}", time.elapsed());
    println!("Got response from child: {}", response);

    executor.shutdown().unwrap();
    //tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}

async fn child() {
    ConcurrentExecutor::<MpTask>::run_process_client().await
}

fn main() {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build_local(LocalOptions::default())
        .unwrap();

    rt.block_on(async move {
        let std_args: Vec<String> = std::env::args().collect();
        if std_args.len() > 1 && std_args[1] == "--child" {
            // Child process
            println!("Starting child process");
            child().await;
        } else {
            host().await;
        }
    });
}