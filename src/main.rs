mod args;
mod tokens;

#[cfg(feature = "auth")]
use std::{collections::HashSet, fs::read_to_string};

use std::sync::{Arc, Mutex};

use actix_web::{
    App, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer,
    http::header,
    mime,
    web::{Data, to},
};
use anyhow::Context;
use awc::{
    Client, ClientBuilder, ClientResponse, error::HeaderValue, http::StatusCode,
};
use clap::Parser;
use futures_util::StreamExt;
use log::{debug, info, warn};

use crate::tokens::{Token, TokensBalencer};

const BASE_URL: &str = "https://openrouter.ai";

struct State {
    client: Client,
    balancer: Arc<Mutex<TokensBalencer>>,
    #[cfg(feature = "auth")]
    allow: Arc<HashSet<String>>,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args: args::Args = args::Args::parse();
    let tokens: Vec<Token> = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .quoting(true)
        .from_path(args.tokens)
        .context("Cannot open the tokens file")?
        .deserialize()
        .collect::<Result<Vec<Token>, csv::Error>>()
        .context("Invalid tokens file")?;
    let balancer: Arc<Mutex<TokensBalencer>> =
        Arc::new(Mutex::new(TokensBalencer::from(tokens)));
    #[cfg(feature = "auth")]
    let allow: Arc<HashSet<String>> = Arc::new(
        read_to_string(args.allow)
            .context("Invalid allow file")?
            .split_whitespace()
            .map(|s| s.to_string())
            .collect(),
    );
    HttpServer::new(move || {
        App::new()
            .app_data(Data::new(State {
                client: ClientBuilder::new().disable_timeout().finish(),
                balancer: Arc::clone(&balancer),
                #[cfg(feature = "auth")]
                allow: Arc::clone(&allow),
            }))
            .default_service(to(default))
    })
    .bind((args.address, args.port))?
    .run()
    .await?;

    Ok(())
}

async fn default(
    req: HttpRequest,
    body: String,
    state: Data<State>,
) -> HttpResponse {
    let peer = req.peer_addr().map(|a| a.to_string()).unwrap_or_else(|| "unknown".to_string());
    info!("[INCOMING] {} {} from {}", req.method(), req.uri(), peer);
    debug!("[INCOMING] headers: {:?}", req.headers());
    debug!("[INCOMING] body: {}", body);
    #[cfg(feature = "auth")]
    {
        let token_result: Result<(), &str> = req
            .headers()
            .get(header::AUTHORIZATION)
            .ok_or("Unauthorized token")
            .and_then(|v| v.to_str().map_err(|_| "Unauthorized token"))
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(s))
            .and_then(|token| {
                if state.allow.contains(token) {
                    Ok(())
                } else {
                    Err("Unauthorized token")
                }
            });

        if let Err(_) = token_result {
            info!("[RESPONSE] 401 Unauthorized");
            return HttpResponse::Unauthorized()
                .insert_header(header::ContentType(mime::APPLICATION_JSON))
                .body(
                    r#"{"error":{"code":401,"message":"Unauthorized token"}}"#,
                );
        }
    }

    let token: Token = match state.balancer.lock() {
        Ok(mut balancer) => {
            match balancer.next() {
                Some(t) => t,
                None => {
                    info!("[RESPONSE] 503 No more tokens");
                    return HttpResponse::ServiceUnavailable()
                    .insert_header(header::ContentType(mime::APPLICATION_JSON))
                    .body(r#"{"error":{"code":503,"message":"No more tokens"}}"#);
                }
            }
        }
        Err(_) => {
            info!("[RESPONSE] 500 Unable to lock the tokens balancer");
            return HttpResponse::InternalServerError()
                .insert_header(header::ContentType(mime::APPLICATION_JSON))
                .body(r#"{"error":{"code":500,"message":"Unable to lock the tokens balancer"}}"#);
        }
    };

    let target_url = format!("{BASE_URL}{}", req.uri());
    debug!("[OPENROUTER] request body: {}", body);
    let mut result: ClientResponse<_> = match state
        .client
        .request_from(&target_url, req.head())
        .insert_header((
            header::AUTHORIZATION,
            format!("Bearer {}", token.token),
        ))
        .send_body(body)
        .await
    {
        Ok(r) => {
            info!("[OPENROUTER] {} {} -> {} (token: {})", req.method(), target_url, r.status(), token.name);
            debug!("[OPENROUTER] response headers: {:?}", r.headers());
            r
        }
        Err(e) => {
            info!("[OPENROUTER] {} {} -> error: {}", req.method(), target_url, e);
            return HttpResponse::InternalServerError()
                .insert_header(header::ContentType(mime::APPLICATION_JSON))
                .body(format!(
                    r#"{{"error":{{"code":500,"message":"{}"}}}}"#,
                    e
                ));
        }
    };

    let status: StatusCode = result.status();
    let token_name: String = token.name.clone();

    let is_chunked: bool = result
        .headers()
        .get(header::TRANSFER_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("chunked"))
        .unwrap_or(false);

    let content_type: Option<&HeaderValue> =
        result.headers().get(header::CONTENT_TYPE);

    let is_sse: bool = content_type
        .map(|v| {
            v.to_str()
                .ok()
                .is_some_and(|v| v.contains("text/event-stream"))
        })
        .unwrap_or(false);

    let mut response: HttpResponseBuilder = HttpResponseBuilder::new(status);
    response.insert_header(("X-TOKEN-NAME", token_name));

    if let Some(ct) = content_type {
        response.insert_header((header::CONTENT_TYPE, ct));
    } else {
        response.insert_header(header::ContentType(mime::APPLICATION_JSON));
    }

    if is_chunked || is_sse {
        info!("[RESPONSE] {} {} (streaming)", status, req.uri());
        let uri = req.uri().to_string();
        let stream = result.map(move |chunk_result| {
            chunk_result
                .map(|chunk| {
                    if is_sse {
                        if let Ok(text) = std::str::from_utf8(&chunk) {
                            for line in text.lines() {
                                let data = line.strip_prefix("data: ").unwrap_or(line);
                                if data.starts_with('{') && data.contains("\"error\":") {
                                    warn!("[STREAM] {} mid-stream error: {}", uri, data);
                                }
                            }
                        }
                    }
                    chunk
                })
                .map_err(|e| {
                    actix_web::error::ErrorInternalServerError(format!(
                        "Stream error: {}",
                        e
                    ))
                })
        });
        response.streaming(stream)
    } else {
        let body = match result.body().await {
            Ok(b) => b,
            Err(e) => {
                info!("[RESPONSE] 500 body read error: {}", e);
                return HttpResponse::InternalServerError()
                    .insert_header(header::ContentType(mime::APPLICATION_JSON))
                    .body(format!(
                        r#"{{"error":{{"code":500,"message":"{}"}}}}"#,
                        e
                    ));
            }
        };
        info!("[RESPONSE] {} {}", status, req.uri());
        debug!("[RESPONSE] body: {}", String::from_utf8_lossy(&body));
        response.body(body)
    }
}
