use concurrentlyexec::ClientContext;
use concurrentlyexec::ConcurrentExecutor;
use concurrentlyexec::ConcurrentExecutorState;
use concurrentlyexec::ConcurrentlyExecute;
use concurrentlyexec::ProcessOpts;
use serde::Deserialize;
use serde::Serialize;
use tokio::runtime::LocalOptions;
use tokio::runtime::Builder;

#[derive(Serialize, Deserialize)]
struct TestStruct {
    a: u32,
    b: String,
}

#[derive(Serialize, Deserialize)]
enum MpTaskMessage {
    Request {
        msg: String, // Some string from the server
        resp: concurrentlyexec::OneshotSender<String>, // A oneshot channel to send the response
    },
    MultiplyByTen {
        x: i32,
        resp: concurrentlyexec::OneshotSender<i32>,
    },
    AddToTestStruct {
        s: TestStruct,
        resp: concurrentlyexec::OneshotSender<TestStruct>,
    },
}

// A really dumb task
#[derive(Clone)]
struct MpTask {}

impl ConcurrentlyExecute for MpTask {
    type Message = MpTaskMessage;

    async fn run(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<concurrentlyexec::Message<Self>>, 
        ctx: ClientContext,
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
                                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                                let _ = resp.send(&ctx, "Long task done".to_string());
                                continue;
                            }
                            let _ = resp.send(&ctx, format!("Echo: {}", msg));
                        }
                        concurrentlyexec::Message::Data { data: MpTaskMessage::MultiplyByTen { x, resp } } => {
                            let _ = resp.send(&ctx, x * 10);
                        }
                        concurrentlyexec::Message::Data { data: MpTaskMessage::AddToTestStruct { s, resp } } => {
                            let mut ns = s;
                            ns.a += 10;
                            ns.b = format!("{} (modified)", ns.b);
                            let _ = resp.send(&ctx, ns);
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

    println!("====== Starting x10 ======");
    let time = std::time::Instant::now();
    let (tx, rx) = executor.create_oneshot();
    executor.send(MpTaskMessage::MultiplyByTen { x: 5, resp: tx }).unwrap();
    println!("Time taken [send]: {:?}", time.elapsed());
    let response = rx.recv().await.unwrap();
    println!("Time taken: {:?}", time.elapsed());
    println!("Got response from child: {}", response);
    assert!(response == 50);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    println!("====== Starting long task ======");
    let time = std::time::Instant::now();
    let (tx, rx) = executor.create_oneshot();
    executor.send(MpTaskMessage::Request { msg: "0".to_string(), resp: tx }).unwrap();
    println!("Time taken [send]: {:?}", time.elapsed());
    let response = rx.recv().await.unwrap();
    println!("Time taken: {:?}", time.elapsed());
    println!("Got response from child: {}", response);

    println!("====== Starting TestStruct modification ======");
    let time = std::time::Instant::now();
    let (tx, rx) = executor.create_oneshot();
    executor.send(MpTaskMessage::AddToTestStruct { s: TestStruct { a: 5, b: "Hello".to_string() }, resp: tx }).unwrap();
    println!("Time taken [send]: {:?}", time.elapsed());
    let response = rx.recv().await.unwrap();
    println!("Time taken: {:?}", time.elapsed());
    println!("Got response from child: a = {}, b = {}", response.a, response.b);
    assert!(response.a == 15);
    assert!(response.b == "Hello (modified)");
    
    let time = std::time::Instant::now();
    for i in 1..10000 {
        let (tx, rx) = executor.create_oneshot();
        executor.send(MpTaskMessage::MultiplyByTen { x: i, resp: tx }).unwrap();
        let response = rx.recv().await.unwrap();
        assert!(response == i * 10);
    }
    println!("Time taken for bulk x10: {:?}", time.elapsed());

    executor.shutdown().unwrap();
    executor.wait().await.unwrap();

    println!("====== Testing blocking shutdown ======");
    let time = std::time::Instant::now();
    let (tx, rx) = executor.create_oneshot();
    executor.send(MpTaskMessage::AddToTestStruct { s: TestStruct { a: 5, b: "Hello".to_string() }, resp: tx }).unwrap();
    let (tx, _rx) = executor.create_oneshot();
    executor.send(MpTaskMessage::AddToTestStruct { s: TestStruct { a: 5, b: "Hello".to_string() }, resp: tx }).unwrap();
    println!("Time taken [send]: {:?}", time.elapsed());
    let response = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        rx.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    println!("Time taken: {:?}", time.elapsed());
    println!("Got response from child: a = {}, b = {}", response.a, response.b);
    assert!(response.a == 15);
    assert!(response.b == "Hello (modified)");

    //tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
}

async fn child() {
    ConcurrentExecutor::<MpTask>::run_process_client().await
}

fn main() {
    let std_args: Vec<String> = std::env::args().collect();
    if std_args.len() > 1 && std_args[1] == "--child" {
        let rt = Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            println!("Starting child process");
            child().await;
        });

        return;
    }


    let rt = Builder::new_current_thread()
        .enable_all()
        .build_local(LocalOptions::default())
        .unwrap();

    rt.block_on(async move {
        host().await;
    });
}