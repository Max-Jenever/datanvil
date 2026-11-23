use infrastructure::redis_dedup::RedisDeduplication;
use redis::Client;
use redis::AsyncCommands;

// Вспомогательная функция для очистки ключа перед тестом
async fn cleanup_key(client: &Client, key: &str) {
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .unwrap_or(());
}

#[tokio::test]
async fn test_deduplication_new_event_accepted() {
    let client = Client::open("redis://localhost:6379").expect("Redis not available");
    let conn = client.get_multiplexed_async_connection().await.expect("Failed to connect");
    let mut dedup = RedisDeduplication::new(conn, 86400);

    let event_id = "dedup-test-new-1";
    let key = format!("dedup:{}", event_id);

    // Очистка перед тестом через ОТДЕЛЬНОЕ соединение
    cleanup_key(&client, &key).await;

    // Первое событие должно быть принято
    let result = dedup.try_mark_seen(event_id).await.unwrap();
    assert!(result, "New event should be accepted");
    assert_eq!(dedup.dropped_count(), 0);
}

#[tokio::test]
async fn test_deduplication_duplicate_rejected() {
    let client = Client::open("redis://localhost:6379").expect("Redis not available");
    let conn = client.get_multiplexed_async_connection().await.expect("Failed to connect");
    let mut dedup = RedisDeduplication::new(conn, 86400);

    let event_id = "dedup-test-dup-1";
    let key = format!("dedup:{}", event_id);

    // Очистка
    cleanup_key(&client, &key).await;

    // Первый вызов — успех
    assert!(dedup.try_mark_seen(event_id).await.unwrap());

    // Второй вызов с тем же id — дубликат
    let is_new = dedup.try_mark_seen(event_id).await.unwrap();
    assert!(!is_new, "Duplicate event should be rejected");
    assert_eq!(dedup.dropped_count(), 1);

    // Третий вызов — всё ещё дубликат
    let is_new = dedup.try_mark_seen(event_id).await.unwrap();
    assert!(!is_new);
    assert_eq!(dedup.dropped_count(), 2);
}

#[tokio::test]
async fn test_deduplication_different_events_independent() {
    let client = Client::open("redis://localhost:6379").expect("Redis not available");
    let conn = client.get_multiplexed_async_connection().await.expect("Failed to connect");
    let mut dedup = RedisDeduplication::new(conn, 86400);

    let key_a = "dedup:evt-a";
    let key_b = "dedup:evt-b";

    // Очистка
    cleanup_key(&client, key_a).await;
    cleanup_key(&client, key_b).await;

    assert!(dedup.try_mark_seen("evt-a").await.unwrap());
    assert!(dedup.try_mark_seen("evt-b").await.unwrap());

    // evt-a повторно — дубликат
    assert!(!dedup.try_mark_seen("evt-a").await.unwrap());

    // evt-b повторно — дубликат
    assert!(!dedup.try_mark_seen("evt-b").await.unwrap());

    assert_eq!(dedup.dropped_count(), 2);
}
