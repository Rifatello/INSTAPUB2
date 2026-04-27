from __future__ import annotations

import json
import os
import random
import re
import subprocess
import textwrap
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException
from openai import OpenAI
from pydantic import BaseModel


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
STORAGE_DIR = BASE_DIR / "storage"
HOOK_PROMPTS_DIR = DATA_DIR / "hook_prompts"
DESCRIPTION_PROMPTS_DIR = DATA_DIR / "description_prompts"
SUPPORTED_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}
SUPPORTED_MUSIC_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".mp4"}
DEFAULT_CTA = "Comment AURA and I will send the full breakdown"
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "/opt/homebrew/opt/ffmpeg-full/bin/ffmpeg")
FFPROBE_BIN = os.getenv(
    "FFPROBE_BIN",
    str((Path(FFMPEG_BIN).parent / "ffprobe")) if Path(FFMPEG_BIN).parent else "ffprobe",
)


class GeneratePreviewRequest(BaseModel):
    account_id: str
    mode: str = "ai"  # "ai" or "generator"


class GeneratePreviewResponse(BaseModel):
    preview_id: str
    account_id: str
    source_video: str
    preview_video: str
    hook: str
    description: str
    caption: str
    status: str
    prompt_source: Optional[str] = None


class GenerateViralHooksRequest(BaseModel):
    account_id: str
    count: int = 12


class GenerateViralHooksResponse(BaseModel):
    account_id: str
    hooks: list[str]
    source: str


class RegeneratePreviewRequest(BaseModel):
    preview_id: str
    refresh: str  # hook | description | music
    mode: str = "ai"


app = FastAPI(title="Content API", version="0.1.0")


VIRAL_HOOK_PATTERNS = [
    "A former {role} told me the one {artifact} the public always misunderstands",
    "A retired {role} tracked {number} cases and found the same mistake repeats first",
    "An ex-{role} showed me why most people lose before they make the big decision",
    "A former {role} said every collapse starts with one small signal people ignore",
    "A {role} who reviewed {number} files said this is where confidence quietly breaks",
    "A retired {role} explained the hidden rule that decides who gets the upside",
    "An ex-{role} said the real risk is not what people panic about first",
    "A former {role} told me the one question that exposes fake certainty fast",
    "A {role} who advised {number} teams said this is where good plans quietly fail",
    "A retired {role} said the first wrong assumption costs more than the big mistake",
]

VIRAL_HOOK_ROLES = [
    "FBI negotiator",
    "hedge fund manager",
    "federal reserve economist",
    "divorce attorney",
    "Swiss bank risk officer",
    "casino mathematician",
    "jury consultant",
    "forensic accountant",
    "family therapist",
    "prison psychologist",
]

VIRAL_HOOK_ARTIFACTS = [
    "pattern",
    "signal",
    "rule",
    "decision",
    "mistake",
    "mechanism",
    "tradeoff",
]

VIRAL_HOOK_NUMBERS = ["120", "300", "900", "1200", "5000", "10000"]
WEAK_HOOK_PHRASES = {
    "one simple framework",
    "mindset shift",
    "for 7 days",
    "most people fail",
    "stop doing this",
}

COMMON_STOPWORDS = {
    "the", "and", "that", "this", "with", "from", "into", "your", "you", "for", "are", "was", "were",
    "have", "has", "had", "not", "but", "they", "their", "them", "who", "why", "what", "when", "how",
    "where", "been", "will", "just", "over", "more", "than", "most", "into", "out", "his", "her",
    "former", "retired", "ex", "said", "told", "showed", "explained", "revealed",
    "first", "wrong", "big", "mistake",
}


def _read_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def _load_accounts() -> list[dict[str, Any]]:
    accounts = _read_json(DATA_DIR / "accounts.json")
    if not isinstance(accounts, list):
        raise ValueError("accounts.json must contain a list")
    return accounts


def _get_account(account_id: str) -> dict[str, Any]:
    for account in _load_accounts():
        if account.get("account_id") == account_id:
            if account.get("status") != "active":
                raise HTTPException(status_code=400, detail="Account is not active")
            return account
    raise HTTPException(status_code=404, detail=f"Account not found: {account_id}")


def _choose_source_video(account: dict[str, Any]) -> Path:
    folder = Path(str(account["video_folder"]))
    if not folder.is_absolute():
        folder = BASE_DIR / folder
    if not folder.exists():
        raise HTTPException(status_code=400, detail=f"Video folder not found: {folder}")

    candidates = [
        p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS
    ]
    if not candidates:
        raise HTTPException(status_code=400, detail=f"No source videos in {folder}")
    return random.choice(candidates)


def choose_music_track(account: dict[str, Any], exclude_track: Path | None = None) -> Path | None:
    music_folder_value = str(account.get("music_folder", "")).strip()
    if not music_folder_value:
        return None
    folder = Path(music_folder_value)
    if not folder.is_absolute():
        folder = BASE_DIR / folder
    if not folder.exists():
        return None

    candidates = [
        p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_MUSIC_EXTENSIONS
    ]
    if exclude_track is not None:
        exclude_resolved = str(exclude_track.resolve()) if exclude_track.exists() else str(exclude_track)
        filtered = []
        for track in candidates:
            track_resolved = str(track.resolve()) if track.exists() else str(track)
            if track_resolved != exclude_resolved:
                filtered.append(track)
        candidates = filtered or candidates

    if not candidates:
        return None
    return random.choice(candidates)


def _load_text_library(filename: str) -> list[str]:
    data = _read_json(DATA_DIR / filename)
    if not isinstance(data, list) or not data:
        raise HTTPException(status_code=500, detail=f"{filename} must be a non-empty list")
    return [str(x).strip() for x in data if str(x).strip()]


def _build_caption(hook: str, description: str) -> str:
    # The description now already includes the CTA at the end.
    return description


def _pick_examples(items: list[str], k: int = 3) -> list[str]:
    if not items:
        return []
    return random.sample(items, k=min(k, len(items)))


def _dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        item = re.sub(r"\s+", " ", str(raw or "").strip())
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _is_weak_hook(hook: str) -> bool:
    low = hook.strip().lower()
    if not low:
        return True
    if len(low) < 35:
        return True
    return any(p in low for p in WEAK_HOOK_PHRASES)


def _fallback_viral_hooks(count: int) -> list[str]:
    hooks: list[str] = []
    for _ in range(max(count * 2, 12)):
        pattern = random.choice(VIRAL_HOOK_PATTERNS)
        candidate = pattern.format(
            role=random.choice(VIRAL_HOOK_ROLES),
            artifact=random.choice(VIRAL_HOOK_ARTIFACTS),
            number=random.choice(VIRAL_HOOK_NUMBERS),
        )
        candidate = candidate.strip().rstrip(".!?") + "."
        hooks.append(candidate)
        if len(_dedupe_keep_order(hooks)) >= count:
            break
    return _dedupe_keep_order(hooks)[:count]


def _load_account_hook_prompt(account_id: str) -> str:
    prompt_file = HOOK_PROMPTS_DIR / account_id / "prompt.txt"
    if not prompt_file.exists():
        return ""
    try:
        text = prompt_file.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
    return text


def _load_account_description_prompt(account_id: str) -> tuple[str, str]:
    account_dir = DESCRIPTION_PROMPTS_DIR / account_id
    if not account_dir.exists():
        return "", "none"
    
    candidates = [] # list of (path, name)
    
    # 2. Check 7 slots
    for i in range(1, 8):
        slot_file = account_dir / f"prompt_slot{i}.txt"
        if slot_file.exists():
            candidates.append((slot_file, f"Slot #{i}"))
            
    # 1. Check legacy prompt.txt (ONLY if no slots are found)
    if not candidates:
        legacy = account_dir / "prompt.txt"
        if legacy.exists():
            candidates.append((legacy, "Legacy"))

    if not candidates:
        return "", "none"
        
    # Choose a random non-empty prompt from available candidates
    valid_prompts = []
    for p, name in candidates:
        try:
            text = p.read_text(encoding="utf-8").strip()
            if text:
                valid_prompts.append((text, name))
        except Exception:
            continue
            
    if not valid_prompts:
        return "", "none"
        
    chosen_text, chosen_name = random.choice(valid_prompts)
    print(f"DEBUG: Selected random description prompt: {chosen_name} (total available: {len(valid_prompts)})")
    return chosen_text, chosen_name


def _load_account_description_examples(account_id: str) -> list[str]:
    examples_file = DESCRIPTION_PROMPTS_DIR / account_id / "examples.txt"
    if not examples_file.exists():
        return []
    try:
        raw = examples_file.read_text(encoding="utf-8")
    except Exception:
        return []
    blocks = [b.strip() for b in raw.split("\n\n") if b.strip()]
    # Keep only meaningful chunks to use as style references.
    out = [b for b in blocks if 40 <= len(b) <= 1200]
    return _dedupe_keep_order(out)


def generate_content_with_generator(
    hooks_examples: list[str],
    descriptions_examples: list[str],
    min_words: int = 60,
    require_five_blocks: bool = False,
    hook: str = "",
) -> dict[str, Any]:
    """
    Template-based description generator. No LLM required.
    Produces a hook-themed, narrative description using rotating
    template families. Always marks itself as 'Генератор описаний'.
    """
    if not hook:
        viral = _fallback_viral_hooks(6)
        hook = random.choice(viral) if viral else (
            random.choice(hooks_examples) if hooks_examples else
            "A former expert revealed the hidden pattern most people miss."
        )

    # Extract key term from hook to inject into the description
    key_term = _extract_hook_key_term(hook)
    
    # 2. Build a unique description by mixing sentences/blocks from multiple examples
    all_sentences = []
    for desc in descriptions_examples:
        parts = re.split(r"(?<=[.!?])\s+|\n\n", desc)
        all_sentences.extend([p.strip() for p in parts if len(p.strip()) > 15])
        
    if not all_sentences:
        all_sentences = ["Это уникальное описание, созданное генератором.", "Здесь скрыт важный смысл."]

    random.shuffle(all_sentences)
    
    # Inject hook key term into some sentences to tie them to the hook
    for i in range(len(all_sentences)):
        if random.random() < 0.3 and " " in all_sentences[i]:
            words = all_sentences[i].split()
            insert_idx = random.randint(1, len(words) - 1)
            words.insert(insert_idx, f"({key_term})")
            all_sentences[i] = " ".join(words)

    result_blocks = []
    num_blocks = 5 if require_five_blocks else random.randint(3, 6)
    
    for i in range(num_blocks):
        chunk_size = random.randint(2, 3)
        chunk = all_sentences[:chunk_size]
        all_sentences = all_sentences[chunk_size:]
        
        if not chunk:
            break
            
        block_text = " ".join(chunk)
        block_text = re.sub(r"^\d+\.\s*", "", block_text).strip()
        
        if require_five_blocks:
            result_blocks.append(f"{i+1}. {block_text}")
        else:
            result_blocks.append(block_text)
            
    description = "\n\n".join(result_blocks)
    if DEFAULT_CTA not in description:
        description += f"\n\n{DEFAULT_CTA}"

    return {
        "hook": hook,
        "description": description,
        "caption": description,
        "prompt_source": "Генератор описаний",
    }


def _description_matches_hook(hook: str, description: str) -> bool:
    hook_tokens = {
        t for t in re.findall(r"[a-zA-Z]{4,}", hook.lower())
        if t not in COMMON_STOPWORDS
    }
    desc_tokens = {
        t for t in re.findall(r"[a-zA-Z]{4,}", description.lower())
        if t not in COMMON_STOPWORDS
    }
    if not hook_tokens or not desc_tokens:
        return False
    return len(hook_tokens.intersection(desc_tokens)) >= 1


def _parse_hooks_json(raw_text: str) -> list[str]:
    parsed = _extract_json_content(raw_text)
    hooks = parsed.get("hooks", [])
    if not isinstance(hooks, list):
        return []
    return [str(x).strip() for x in hooks if str(x).strip()]


def generate_viral_hooks(
    account: dict[str, Any],
    hooks_examples: list[str],
    count: int = 12,
    recent_hooks: list[str] | None = None,
) -> tuple[list[str], str]:
    max_count = max(1, min(int(count), 30))
    recent_set = {str(x).strip().lower() for x in (recent_hooks or []) if str(x).strip()}
    fallback = _fallback_viral_hooks(max_count)
    account_id = str(account.get("account_id", "")).strip()
    account_prompt = _load_account_hook_prompt(account_id)
    force_uppercase = "uppercase" in account_prompt.lower()
    if force_uppercase:
        fallback = [x.upper() for x in fallback]
    api_key, base_url, model = _get_llm_config()
    if not api_key:
        return fallback, "fallback"

    theme = str(account.get("theme", "")).strip()
    style = str(account.get("style", "")).strip()
    examples_block = "\n".join(f"- {x}" for x in _dedupe_keep_order(hooks_examples)[:10])
    avoid_block = "\n".join(f"- {x}" for x in list(recent_set)[:20])

    system_prompt = (
        "You write viral short-form video hooks. "
        "Return ONLY JSON: {\"hooks\": [\"...\"]}. "
        "No markdown, no explanation."
    )
    user_prompt = (
        f"Theme: {theme}\n"
        f"Style: {style}\n\n"
        "Goal: Generate high-retention hooks with authority + hidden mechanism + curiosity gap.\n"
        "Rules:\n"
        "1) Single sentence, 9-18 words.\n"
        "2) Concrete framing: former/ex/retired expert, tracked data, hidden signal, or first mistake.\n"
        "3) No generic motivational phrases.\n"
        "4) No emojis, hashtags, or quotes.\n"
        "5) Keep them specific and sharp.\n"
        f"6) Return exactly {max_count} hooks.\n\n"
        "Style references:\n"
        f"{examples_block}\n\n"
        "Avoid repeating these hooks:\n"
        f"{avoid_block}\n"
        + ("\nAccount-specific hook prompt:\n" + account_prompt + "\n" if account_prompt else "")
    )

    try:
        if not api_key:
            print("LLM Error: No API key found.")
            raise ValueError("No API key")
            
        client = OpenAI(api_key=api_key, base_url=base_url)
        print(f"Calling LLM for viral hooks using model: {model}")
        
        response = client.chat.completions.create(
            model=model,
            temperature=0.95,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        raw_text = (response.choices[0].message.content or "").strip()
        parsed_hooks = _parse_hooks_json(raw_text)
        cleaned: list[str] = []
        for hook in parsed_hooks:
            candidate = re.sub(r"\s+", " ", hook).strip()
            if not candidate:
                continue
            if candidate[-1] not in ".!?":
                candidate += "."
            if candidate.lower() in recent_set:
                continue
            if _is_weak_hook(candidate):
                continue
            cleaned.append(candidate)

        unique_cleaned = _dedupe_keep_order(cleaned)
        if force_uppercase:
            unique_cleaned = [x.upper() for x in unique_cleaned]
        if not unique_cleaned:
            return fallback, "fallback"
        if len(unique_cleaned) < max_count:
            topup = [x for x in fallback if x.lower() not in {u.lower() for u in unique_cleaned}]
            unique_cleaned.extend(topup)
        return unique_cleaned[:max_count], "llm"
    except Exception as e:
        print(f"LLM call failed: {str(e)}. Using fallback logic.")
        return fallback, "fallback"


def _extract_hook_key_term(hook: str) -> str:
    quoted = re.findall(r"[\"“](.*?)[\"”]", hook)
    if quoted:
        return quoted[0].strip().lower()
    hook_tokens = [
        t for t in re.findall(r"[a-zA-Z]{4,}", hook.lower())
        if t not in COMMON_STOPWORDS
    ]
    return hook_tokens[-1] if hook_tokens else "this pattern"


def _word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z]+", text))


def _extract_min_words(prompt_text: str, default_words: int = 60) -> int:
    low = prompt_text.lower()
    patterns = [
        r"at least\s+(\d+)\s+words",
        r"not less than\s+(\d+)\s+words",
        r"(\d+)\s*\+\s*words",
        r"не\s+менее\s+(\d+)\s+слов",
    ]
    values: list[int] = []
    for p in patterns:
        for m in re.findall(p, low):
            try:
                values.append(int(m))
            except Exception:
                continue
    return max(values) if values else default_words


def _requires_five_blocks(prompt_text: str) -> bool:
    low = prompt_text.lower()
    return any(
        x in low
        for x in (
            "5 slides",
            "exactly 5",
            "5 blocks",
            "5 смысловых блок",
            "ровно на 5",
        )
    )


def _has_required_block_markers(description: str) -> bool:
    text = "\n\n" + description
    return all(f"\n\n{n}. " in text for n in (2, 3, 4, 5))


def _sample_style_sentences(descriptions_examples: list[str], max_items: int = 3) -> list[str]:
    out: list[str] = []
    for block in descriptions_examples:
        sentence = re.split(r"(?<=[.!?])\s+", block.strip())[0].strip()
        if 30 <= len(sentence) <= 200:
            out.append(sentence)
        if len(out) >= max_items:
            break
    return out


def _build_structured_fallback_description(
    hook: str,
    descriptions_examples: list[str],
    min_words: int,
    require_five_blocks: bool,
) -> str:
    # If we have real examples, use one of them as a baseline for the fallback
    if descriptions_examples:
        # Pick a random example that is long enough
        valid_examples = [e for e in descriptions_examples if _word_count(e) >= 40]
        if valid_examples:
            desc = random.choice(valid_examples).strip()
            # Clean up old Russian CTA if present in examples
            desc = desc.replace("👉 Ссылка в био", "").strip()
            if DEFAULT_CTA not in desc:
                desc = f"{desc}\n\n{DEFAULT_CTA}"
            return desc

    key_term = _extract_hook_key_term(hook)
    
    # Selection of different base stories for the fallback
    templates = [
        # Template 1: FBI/Negotiator (Original)
        [
            "He worked in rooms where money, status, and pressure met before anyone said a word.\n\n"
            "After enough nights, he stopped reading outfits and started reading frame, pace, and silence.\n\n"
            f"That is why this hook matters: {key_term} is not a slogan, it is a live mechanism.",
            "2. Most people think outcomes are decided by arguments.",
            "He said outcomes are usually decided earlier, when one side controls tempo and the other side starts explaining too much.\n\n"
            "The hidden pattern is simple: when tension rises, amateurs speed up, professionals reduce motion.",
            "3. In one meeting, two founders answered the same pushback in opposite ways.",
            "The first filled every pause, added extra context, and kept signaling that he wanted approval.",
            "The second slowed his ending, kept one clean sentence, and let silence hold for one beat.",
            "4. Once the frame shifts, the room changes behavior without announcing it.",
            "Questions become shorter, resistance becomes softer, and weak attacks start self-correcting.",
            "5. The cold rule is not to speak more, but to leak less.\n\n"
            "Pick your ground, answer only what was asked, finish the sentence, and do not rescue every silence."
        ],
        # Template 2: Mayfair Club/Observer
        [
            "He worked nights at a Mayfair club where founders, athletes, and old family money used the same staircase.\n\n"
            "After enough shifts he stopped watching clothes and started watching entrances.\n\n"
            f"That is where men give the game away, and {key_term} becomes the only thing that matters.",
            "2. Some people are loud because they like noise. Some are loud because they need confirmation.",
            "The ones trying to own the room checked everything at once: who noticed them, which table had the best angle.",
            "It felt less like confidence and more like somebody scanning for proof.",
            "3. You can tell who wants authority and who assumes it within ten steps.",
            "He remembers a retired investor who chose the side wall, sat down, and talked to one person at a time.",
            "By midnight people were lining up to reach the quieter man.",
            "4. The center is a trap for men who want to feel significant fast.",
            "If you have to plant yourself in the brightest part of the room, everybody feels the hunger before they feel the charm.",
            "5. Men with presence let attention come find them.\n\n"
            "Stop entering spaces like you need to win them in the first thirty seconds. That need leaks out."
        ],
        # Template 3: Analyst/Compliance
        [
            "He spent 22 years in financial compliance monitoring accounts for suspicious activity.\n\n"
            "The threshold most people know is $10,000. Но он говорит, что это всего лишь прикрытие.\n\n"
            f"The real triggers are pattern-based, specifically how {key_term} deviates from the norm.",
            "2. Multiple deposits of $9,500. Round number transfers on repeating schedules.",
            "His team flagged 340 accounts per month in one branch alone because they missed the hidden rule.",
            "The algorithm compares your behavior to your own history, not to a fixed limit.",
            "3. Your bank files a Suspicious Activity Report without telling you.",
            "No notification. No appeal. It sits in a federal database and changes how the system treats you.",
            "Once filed, your account gets elevated monitoring for 18 months minimum.",
            "4. Anything that looks different from your last 90 days is a flag.",
            "If you normally deposit $3,000 and suddenly deposit $12,000, you are on the list.",
            "5. The people who understand the system build businesses inside it.\n\n"
            "The rest get watched by it. Presence in the digital age is about staying inside your own frame."
        ]
    ]
    
    parts = random.choice(templates)
    description = "\n\n".join(parts)

    filler_pool = [
        "Under pressure, people reveal whether they control tension or are controlled by it.",
        "The room rarely rewards noise; it rewards stable pace and clean endings.",
        "When someone stops performing for approval, negotiations become more factual and less emotional.",
        "Presence is often just disciplined timing repeated across uncomfortable moments.",
        "Silence exposes deviation faster than aggressive words ever can.",
    ]
    i = 0
    while _word_count(description) < min_words and i < len(filler_pool):
        description += "\n\n" + filler_pool[i]
        i += 1
    
    # Ensure CTA is at the very end
    return f"{description.strip()}\n\n{DEFAULT_CTA}"


def _get_llm_config() -> tuple[str, str, str]:
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    
    # Check for DeepSeek first if explicit
    if provider == "deepseek":
        api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
        base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1").strip()
        model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip()
        return api_key, base_url, model

    # Default to OpenAI env vars but check for DeepSeek URL
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
    
    # Auto-detect DeepSeek if URL points there
    if "deepseek.com" in base_url.lower():
        model = os.getenv("OPENAI_MODEL", "deepseek-chat").strip()
    else:
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
        
    return api_key, base_url, model


def _extract_json_content(raw_text: str) -> dict[str, Any]:
    raw_text = raw_text.strip()
    if not raw_text:
        raise ValueError("Empty LLM response")

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("LLM response is not valid JSON")
        return json.loads(raw_text[start : end + 1])


def _is_valid_generated_content(content: dict[str, Any], min_words: int = 40, require_five_blocks: bool = False) -> bool:
    hook = str(content.get("hook", "")).strip()
    description = str(content.get("description", "")).strip()
    caption = str(content.get("caption", "")).strip()
    return bool(
        hook
        and description
        and caption
        and len(hook) >= 20
        and _word_count(description) >= min_words
        and _description_matches_hook(hook, description)
        and (not require_five_blocks or _has_required_block_markers(description))
        and DEFAULT_CTA in caption
    )


def generate_content(
    account: dict[str, Any],
    hooks_examples: list[str],
    descriptions_examples: list[str],
    recent_hooks: list[str] | None = None,
) -> dict[str, str]:
    account_id = str(account.get("account_id", "")).strip()
    account_hook_prompt = _load_account_hook_prompt(account_id)
    account_description_prompt, prompt_source = _load_account_description_prompt(account_id)
    force_english = any(
        token in (account_description_prompt + "\n" + account_hook_prompt).lower()
        for token in ("english only", "strictly in english")
    )

    viral_candidates, _ = generate_viral_hooks(
        account=account,
        hooks_examples=hooks_examples,
        count=6,
        recent_hooks=recent_hooks,
    )
    hook = (random.choice(viral_candidates) if viral_candidates else random.choice(hooks_examples)).strip()
    min_words = _extract_min_words(account_description_prompt, default_words=60)
    require_five_blocks = _requires_five_blocks(account_description_prompt)
    
    try:
        api_key, base_url, model = _get_llm_config()
        if not api_key:
            raise ValueError("No API key")
            
        system_prompt = (
            "You are an elite social copywriter. "
            "Return ONLY JSON with keys: hook, description, caption. "
            "No markdown. No explanations."
        )
        user_prompt = (
            f"Account theme: {account.get('theme', '')}\n"
            f"Account style: {account.get('style', '')}\n\n"
            + ("Output language: ENGLISH ONLY.\n\n" if force_english else "")
            + f"Target hook:\n{hook}\n\n"
            + "Task:\n"
            + "STRICT RULE: Write a unique, original story specifically based on the provided hook mechanism. Never reuse stories from examples. Every hook deserves its own fresh narrative logic.\n"
            + "1) Use the provided hook for the 'hook' key, but DO NOT include it in the 'description' text.\n"
            + "2) Write a long description that directly expands the same mechanism as the hook.\n"
            + f"3) Description length must be at least {min_words} words.\n"
            + "4) Keep cinematic pacing with short paragraphs and blank lines between them.\n"
            + ("5) Use exactly 5 blocks: first block unnumbered, then blocks 2., 3., 4., 5.\n" if require_five_blocks else "")
            + "6) Do NOT include any call to action (CTA) in the description text.\n"
            + f"7) Build caption exactly as: description + blank line + {DEFAULT_CTA}\n"
            + "8) Return only JSON."
        )

        client = OpenAI(api_key=api_key, base_url=base_url)
        print(f"Calling LLM for full content using model: {model}")
        response = client.chat.completions.create(
            model=model,
            temperature=0.8,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        raw_text = (response.choices[0].message.content or "").strip()
        parsed = _extract_json_content(raw_text)
        
        description = str(parsed.get("description", "")).strip()
        if DEFAULT_CTA not in description:
            description = f"{description}\n\n{DEFAULT_CTA}"
        
        candidate = {
            "hook": hook,
            "description": description,
            "caption": str(parsed.get("caption", "")).strip()
        }
        if _is_valid_generated_content(candidate, min_words=min_words, require_five_blocks=require_five_blocks):
            print(f"LLM Success: Content generated using {prompt_source}.")
            candidate["prompt_source"] = prompt_source
            return candidate
            
    except Exception as e:
        print(f"LLM content generation failed: {str(e)}. Using fallback logic.")
        
    fallback_description = _build_structured_fallback_description(
        hook=hook,
        descriptions_examples=descriptions_examples,
        min_words=min_words,
        require_five_blocks=require_five_blocks,
    )
    return {
        "hook": hook,
        "description": fallback_description,
        "caption": _build_caption(hook, fallback_description),
        "prompt_source": f"{prompt_source} (LLM FAILED - FALLBACK USED)",
    }
    api_key, base_url, model = _get_llm_config()
    if not api_key:
        return fallback

    theme = str(account.get("theme", "")).strip()
    style = str(account.get("style", "")).strip()
    style_examples = _sample_style_sentences(descriptions_examples, max_items=3)

    system_prompt = (
        "You are an elite social copywriter. "
        "Return ONLY JSON with keys: hook, description, caption. "
        "No markdown. No explanations."
    )
    user_prompt = (
        f"Account theme: {theme}\n"
        f"Account style: {style}\n\n"
        + ("Output language: ENGLISH ONLY.\n\n" if force_english else "")
        + f"Target hook:\n{hook}\n\n"
        + "Hook style references:\n"
        + "\n".join(f"- {x}" for x in (viral_candidates[:5] or hooks_examples[:5]))
        + "\n\nDescription style references:\n"
        + "\n".join(f"- {x}" for x in style_examples)
        + ("\n\nAccount hook prompt:\n" + account_hook_prompt if account_hook_prompt else "")
        + ("\n\nAccount description prompt:\n" + account_description_prompt if account_description_prompt else "")
        + "\n\nTask:\n"
        + "1) Use the provided hook for the 'hook' key, but DO NOT include it in the 'description' text.\n"
        + "2) Write a long description that directly expands the same mechanism as the hook.\n"
        + f"3) Description length must be at least {min_words} words.\n"
        + "4) Keep cinematic pacing with short paragraphs and blank lines between them.\n"
        + ("5) Use exactly 5 blocks: first block unnumbered, then blocks 2., 3., 4., 5.\n" if require_five_blocks else "")
        + "6) Do NOT include any call to action (CTA) in the description text.\n"
        + f"7) Build caption exactly as: description + blank line + {DEFAULT_CTA}\n"
        + "8) Return only JSON."
    )

    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        response = client.chat.completions.create(
            model=model,
            temperature=0.8,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        raw_text = (response.choices[0].message.content or "").strip()
        parsed = _extract_json_content(raw_text)

        generated_hook = str(parsed.get("hook", "")).strip()
        description = str(parsed.get("description", "")).strip()
        
        # Ensure CTA is appended to description if not already there
        if DEFAULT_CTA not in description:
            description = f"{description}\n\n{DEFAULT_CTA}"
            
        caption = str(parsed.get("caption", "")).strip()
        candidate_hook = hook if hook else generated_hook
        if (not caption or DEFAULT_CTA not in caption) and description:
            caption = description # Now description already has CTA

        candidate = {"hook": candidate_hook, "description": description, "caption": caption}
        if not _is_valid_generated_content(candidate, min_words=min_words, require_five_blocks=require_five_blocks):
            return fallback
        return candidate
    except Exception:
        return fallback


def _preview_output_path(preview_id: str) -> Path:
    return STORAGE_DIR / "processing" / f"{preview_id}.mp4"


def _escape_drawtext_text(text: str) -> str:
    escaped = text.replace("\\", r"\\")
    escaped = escaped.replace(":", r"\:")
    escaped = escaped.replace("'", r"\'")
    escaped = escaped.replace("%", r"\%")
    return escaped


def _build_drawtext_filter(lines: list[str]) -> str:
    filters = []
    font_path = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
    base_y_expr = "h*0.08"
    font_size = 64
    line_spacing = 16

    for i, line in enumerate(lines):
        line_escaped = _escape_drawtext_text(line)
        # Вычисляем Y для каждой строки отдельно
        y_expr = f"{base_y_expr}+{i}*({font_size}+{line_spacing})"
        f = (
            "drawtext="
            f"fontfile='{font_path}':"
            f"text='{line_escaped}':"
            "fontcolor=white:"
            f"fontsize={font_size}:"
            "bordercolor=black:"
            "borderw=3:"
            "x=(w-text_w)/2:"
            f"y={y_expr}:"
            "fix_bounds=true"
        )
        filters.append(f)

    return ",".join(filters)


def _has_audio_stream(video_path: Path) -> bool:
    command = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        return result.returncode == 0 and bool((result.stdout or "").strip())
    except Exception:
        return False


def _probe_duration_seconds(video_path: Path) -> float:
    command = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        return float((result.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _rand_signed_abs(min_abs: float, max_abs: float) -> float:
    value = random.uniform(min_abs, max_abs)
    return value if random.choice([True, False]) else -value


def _build_unique_video_filter(duration: float, speed: float) -> tuple[str, float, float]:
    # Целевая длительность на выходе: от 5.0 до 8.0 секунд
    # final_duration = segment_length / speed
    # Отсюда segment_length = final_duration * speed
    target_final = random.uniform(5.5, 7.5)
    needed_segment = target_final * speed

    # Если исходник короче, чем нужно даже с учетом скорости, берем весь исходник
    if needed_segment > duration:
        start_val = 0.0
        end_val = duration
    else:
        # Иначе выбираем случайный кусок нужной длины
        max_start = duration - needed_segment
        start_val = random.uniform(0, max_start)
        end_val = start_val + needed_segment

    crop_ratio = random.uniform(0.95, 0.98)
    brightness = _rand_signed_abs(0.02, 0.05)
    contrast = 1.0 + _rand_signed_abs(0.02, 0.05)
    saturation = 1.0 + _rand_signed_abs(0.05, 0.1)
    hue_shift = _rand_signed_abs(1.0, 5.0)
    rotation = _rand_signed_abs(0.002, 0.006)
    fps = random.choice([29.97, 30.05, 30.15])

    vf = (
        f"trim=start={start_val:.3f}:end={end_val:.3f},"
        "setpts=PTS-STARTPTS,"
        f"crop=iw*{crop_ratio:.4f}:ih*{crop_ratio:.4f},"
        f"rotate=a={rotation:.4f}:ow=iw:oh=ih:c=black,"
        f"eq=brightness={brightness:.4f}:contrast={contrast:.4f}:saturation={saturation:.4f},"
        f"hue=h={hue_shift:.3f},"
        "noise=alls=1:allf=t+u,"
        "vignette=PI/100,"
        f"setpts=PTS/{speed:.4f},"
        f"fps={fps},"
        "scale=trunc(iw/2)*2:trunc(ih/2)*2"
    )
    return vf, start_val, end_val


def render_unique_preview(preview_video: Path) -> None:
    duration = _probe_duration_seconds(preview_video)
    if duration <= 0.5:
        raise HTTPException(status_code=500, detail="Cannot uniqueize: invalid video duration")

    has_audio = _has_audio_stream(preview_video)
    
    # Если видео слишком короткое, замедляем его сильнее, чтобы попасть в 5с
    if duration < 5.0:
        speed = random.uniform(0.6, 0.8)
    else:
        speed = random.uniform(0.9, 1.1)

    vf, start_val, end_val = _build_unique_video_filter(duration, speed)
    volume = random.uniform(0.7, 0.9)

    tmp_output = preview_video.with_name(f"{preview_video.stem}.unique.mp4")
    noise_input = "anoisesrc=color=white:amplitude=0.008:sample_rate=44100"

    if has_audio:
        highpass_f = random.randint(100, 250)
        lowpass_f = random.randint(10000, 15000)
        af = (
            f"atrim=start={start_val:.3f}:end={end_val:.3f},"
            "asetpts=PTS-STARTPTS,"
            f"volume={volume:.4f},"
            f"highpass=f={highpass_f},"
            f"lowpass=f={lowpass_f},"
            f"atempo={speed:.4f}"
        )
        filter_complex = (
            f"[0:v]{vf}[vout];"
            f"[0:a]{af}[abase];"
            "[1:a]volume=0.10[anoise];"
            "[abase][anoise]amix=inputs=2:weights='1 0.25':duration=first[aout]"
        )
    else:
        filter_complex = (
            f"[0:v]{vf}[vout];"
            f"[1:a]volume=0.20,atempo={speed:.4f}[aout]"
        )

    command = [
        FFMPEG_BIN,
        "-y",
        "-i",
        str(preview_video),
        "-f",
        "lavfi",
        "-i",
        noise_input,
        "-filter_complex",
        filter_complex,
        "-map",
        "[vout]",
        "-map",
        "[aout]",
        "-shortest",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        str(tmp_output),
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        tmp_output.replace(preview_video)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise HTTPException(status_code=500, detail=f"FFmpeg uniqueize failed: {stderr[-400:]}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Uniqueize failed: {exc}") from exc
    finally:
        if tmp_output.exists():
            try:
                tmp_output.unlink()
            except Exception:
                pass
def _get_video_duration(path: Path) -> float:
    try:
        cmd = [
            FFPROBE_BIN, "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(path)
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return float(res.stdout.strip())
    except Exception:
        return 10.0



def render_preview_with_hook(
    source_video: Path,
    preview_video: Path,
    hook: str,
    music_track: Path | None = None,
    force_9_16: bool = False,
) -> None:
    # Агрессивная очистка от спецсимволов и эмодзи
    clean_hook = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s.,!?;:()"\-]', '', hook)
    clean_hook = clean_hook.strip().replace("\r", "")
    
    # Разбиваем на строки в Python
    lines = [line.strip() for line in textwrap.wrap(clean_hook, width=22)]
    if not lines:
        lines = [""]

    drawtext_filter = _build_drawtext_filter(lines)
    
    # Фильтр для кропа в 9:16 (центровка и приведение к 1080x1920)
    if force_9_16:
        v_filters = f"crop='min(iw,ih*9/16):min(ih,iw*16/9)',scale=1080:1920,{drawtext_filter}"
    else:
        v_filters = drawtext_filter

    duration = _get_video_duration(source_video)
    has_source_audio = _has_audio_stream(source_video)

    if music_track is not None:
        # Improved command: explicit duration, no video from music, stable mapping
        command = [
            FFMPEG_BIN,
            "-y",
            "-i", str(source_video),
            "-stream_loop", "-1",
            "-vn", "-sn", "-dn",
            "-i", str(music_track),
            "-filter_complex", f"[0:v]{v_filters}[vout];[1:a]volume=0.5[aout]",
            "-map", "[vout]",
            "-map", "[aout]",
            "-t", str(duration), # Explicitly limit to video duration instead of -shortest
            "-c:v", "libx264",
            "-preset", "veryfast", # Faster encoding uses less temp resources
            "-c:a", "aac",
            str(preview_video),
        ]
    else:
        command = [
            FFMPEG_BIN, "-y",
            "-i", str(source_video),
            "-vf", v_filters,
            "-t", str(duration)
        ]
        if has_source_audio:
            command.extend(["-c:a", "copy"])
        else:
            command.append("-an")
        command.append(str(preview_video))

    try:
        print("Using ffmpeg binary:", FFMPEG_BIN)
        print("Source has audio:", has_source_audio)
        if music_track is not None:
            print("Using music track:", str(music_track))
        
        # Log the command for debugging
        print("Running FFmpeg command:", " ".join(command))
        
        # Force TMPDIR and other temp variables to project root
        custom_env = os.environ.copy()
        project_tmp = str(BASE_DIR / "storage" / "tmp")
        (BASE_DIR / "storage" / "tmp").mkdir(parents=True, exist_ok=True)
        
        custom_env["TMPDIR"] = project_tmp
        custom_env["TEMP"] = project_tmp
        custom_env["TMP"] = project_tmp
        # Enable FFmpeg report for deep debugging
        custom_env["FFREPORT"] = f"file={project_tmp}/ffmpeg-last-run.log:level=32"
        
        result = subprocess.run(command, check=True, capture_output=True, text=True, env=custom_env)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail="FFmpeg is not installed or not in PATH.") from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        print("FFmpeg stderr:", stderr)
        # Show first 1000 chars in response for the user
        error_start = stderr[:1000] if stderr else "unknown error"
        raise HTTPException(status_code=500, detail=f"FFmpeg render failed: {error_start}") from exc


def _save_preview_state(
    preview_id: str,
    account_id: str,
    source_video: Path,
    preview_video: Path,
    hook: str,
    description: str,
    caption: str,
    music_track: Path | None = None,
) -> None:
    state_path = DATA_DIR / "state.json"
    state = _read_json(state_path)

    pending = state.setdefault("pending_previews", {})
    history = state.setdefault("history", [])
    service = state.setdefault("service", {})
    recent_hooks = state.setdefault("recent_hooks", [])
    recent_descriptions = state.setdefault("recent_descriptions", [])

    now_iso = datetime.now(timezone.utc).isoformat()
    preview_record = {
        "preview_id": preview_id,
        "account_id": account_id,
        "source_video": str(source_video),
        "preview_video": str(preview_video),
        "hook": hook,
        "description": description,
        "caption": caption,
        "music_track": str(music_track) if music_track else None,
        "status": "preview_ready",
        "created_at": now_iso,
    }

    pending[preview_id] = preview_record
    history.append(preview_record)
    recent_hooks.append(hook)
    recent_descriptions.append(description)

    state["recent_hooks"] = recent_hooks[-20:]
    state["recent_descriptions"] = recent_descriptions[-20:]
    state["history"] = history[-200:]
    service["last_preview_id"] = preview_id
    service["updated_at"] = now_iso
    state["service"] = service

    _write_json(state_path, state)


def _build_preview_response(record: dict[str, Any]) -> GeneratePreviewResponse:
    return GeneratePreviewResponse(
        preview_id=str(record["preview_id"]),
        account_id=str(record["account_id"]),
        source_video=str(record["source_video"]),
        preview_video=str(record["preview_video"]),
        hook=str(record["hook"]),
        description=str(record["description"]),
        caption=str(record["caption"]),
        status=str(record.get("status", "preview_ready")),
    )


def _load_pending_preview(preview_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    state_path = DATA_DIR / "state.json"
    state = _read_json(state_path)
    pending = state.setdefault("pending_previews", {})
    if not isinstance(pending, dict):
        raise HTTPException(status_code=500, detail="state.json pending_previews is invalid")

    record = pending.get(preview_id)
    if not isinstance(record, dict):
        raise HTTPException(status_code=404, detail=f"Preview not found: {preview_id}")
    return state, record


def _persist_pending_preview(state: dict[str, Any], record: dict[str, Any]) -> None:
    pending = state.setdefault("pending_previews", {})
    history = state.setdefault("history", [])
    service = state.setdefault("service", {})
    recent_hooks = state.setdefault("recent_hooks", [])
    recent_descriptions = state.setdefault("recent_descriptions", [])

    preview_id = str(record["preview_id"])
    now_iso = datetime.now(timezone.utc).isoformat()
    record["updated_at"] = now_iso
    pending[preview_id] = record
    history.append(record.copy())
    recent_hooks.append(str(record.get("hook", "")))
    recent_descriptions.append(str(record.get("description", "")))
    state["recent_hooks"] = recent_hooks[-20:]
    state["recent_descriptions"] = recent_descriptions[-20:]
    state["history"] = history[-200:]
    service["last_preview_id"] = preview_id
    service["updated_at"] = now_iso
    state["service"] = service
    _write_json(DATA_DIR / "state.json", state)


def _load_recent_hooks(limit: int = 30) -> list[str]:
    state_path = DATA_DIR / "state.json"
    try:
        state = _read_json(state_path)
    except Exception:
        return []
    hooks = state.get("recent_hooks", [])
    if not isinstance(hooks, list):
        return []
    cleaned = [str(x).strip() for x in hooks if str(x).strip()]
    return cleaned[-max(1, limit):]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/generate-viral-hooks", response_model=GenerateViralHooksResponse)
def generate_viral_hooks_endpoint(payload: GenerateViralHooksRequest) -> GenerateViralHooksResponse:
    account = _get_account(payload.account_id)
    hooks_library = _load_text_library("hooks.json")
    examples = _pick_examples(hooks_library, k=min(12, max(3, payload.count)))
    recent_hooks = _load_recent_hooks(limit=40)
    hooks, source = generate_viral_hooks(
        account=account,
        hooks_examples=examples,
        count=payload.count,
        recent_hooks=recent_hooks,
    )
    return GenerateViralHooksResponse(
        account_id=str(account["account_id"]),
        hooks=hooks,
        source=source,
    )


@app.post("/generate-preview", response_model=GeneratePreviewResponse)
def generate_preview(payload: GeneratePreviewRequest) -> GeneratePreviewResponse:
    account = _get_account(payload.account_id)
    source_video = _choose_source_video(account)
    music_track = choose_music_track(account)
    hooks_library = _load_text_library("hooks.json")
    descriptions_library = _load_text_library("descriptions.json")
    account_description_examples = _load_account_description_examples(str(account["account_id"]))
    if account_description_examples:
        descriptions_library = account_description_examples
    recent_hooks = _load_recent_hooks(limit=40)
    hook_examples = _pick_examples(hooks_library, k=3)
    description_examples = _pick_examples(descriptions_library, k=3)
    
    mode = payload.mode.lower()
    if mode == "generator":
        generated = generate_content_with_generator(
            hooks_examples=hooks_library,
            descriptions_examples=descriptions_library,
            min_words=_extract_min_words(_load_account_description_prompt(payload.account_id)[0], 60),
            require_five_blocks=_requires_five_blocks(_load_account_description_prompt(payload.account_id)[0])
        )
    else:
        generated = generate_content(account, hook_examples, description_examples, recent_hooks=recent_hooks)

    hook = generated["hook"].strip()
    description = generated["description"].strip()
    caption = generated["caption"].strip()
    if not hook or not description or not caption:
        fallback = _fallback_content(hooks_library, descriptions_library)
        hook = fallback["hook"]
        description = fallback["description"]
        caption = fallback["caption"]

    preview_id = str(uuid.uuid4())
    preview_video = _preview_output_path(preview_id)
    preview_video.parent.mkdir(parents=True, exist_ok=True)

    # Automatically force 9:16 format 
    render_preview_with_hook(
        source_video=source_video,
        preview_video=preview_video,
        hook=hook,
        music_track=music_track,
        force_9_16=True,  # Force 9:16 format as requested
    )

    # Автоматическая уникализация после рендеринга
    # render_unique_preview(preview_video)

    _save_preview_state(
        preview_id=preview_id,
        account_id=str(account["account_id"]),
        source_video=source_video,
        preview_video=preview_video,
        hook=hook,
        description=description,
        caption=caption,
        music_track=music_track,
    )

    return GeneratePreviewResponse(
        preview_id=preview_id,
        account_id=str(account["account_id"]),
        source_video=str(source_video),
        preview_video=str(preview_video),
        hook=hook,
        description=description,
        caption=caption,
        status="preview_ready",
        prompt_source=generated.get("prompt_source")
    )


@app.post("/regenerate-preview", response_model=GeneratePreviewResponse)
def regenerate_preview(payload: RegeneratePreviewRequest) -> GeneratePreviewResponse:
    refresh_type = payload.refresh.strip().lower()
    if refresh_type not in {"hook", "description", "music", "unique", "format_9_16"}:
        raise HTTPException(
            status_code=400,
            detail="refresh must be one of: hook, description, music, unique, format_9_16",
        )

    state, record = _load_pending_preview(payload.preview_id)
    account = _get_account(str(record.get("account_id", "")))

    source_video = Path(str(record.get("source_video", "")))
    if not source_video.exists():
        raise HTTPException(status_code=400, detail=f"Source video not found: {source_video}")

    preview_video = Path(str(record.get("preview_video", "")))
    preview_video.parent.mkdir(parents=True, exist_ok=True)

    hook = str(record.get("hook", "")).strip()
    description = str(record.get("description", "")).strip()
    music_track = Path(record["music_track"]) if record.get("music_track") else None

    hooks_library = _load_text_library("hooks.json")
    descriptions_library = _load_text_library("descriptions.json")
    account_description_examples = _load_account_description_examples(str(account["account_id"]))
    if account_description_examples:
        descriptions_library = account_description_examples

    mode = payload.mode.lower()
    if refresh_type == "hook":
        if mode == "generator":
            generated = generate_content_with_generator(hooks_library, descriptions_library)
        else:
            generated = generate_content(
                account,
                _pick_examples(hooks_library, 3),
                _pick_examples(descriptions_library, 3),
                recent_hooks=_load_recent_hooks(limit=40),
            )
        hook = str(generated.get("hook", "")).strip() or hook
    elif refresh_type == "description":
        if mode == "generator":
            generated = generate_content_with_generator(hooks_library, descriptions_library)
        else:
            generated = generate_content(
                account,
                _pick_examples(hooks_library, 3),
                _pick_examples(descriptions_library, 3),
                recent_hooks=_load_recent_hooks(limit=40),
            )
        description = str(generated.get("description", "")).strip() or description
    elif refresh_type == "music":
        music_track = choose_music_track(account, exclude_track=music_track)
    elif refresh_type == "unique":
        # keep current text/music, only transform media with randomized params
        # render_unique_preview(preview_video)
        pass
    elif refresh_type == "format_9_16":
        record["is_9_16"] = True

    caption = _build_caption(hook, description)
    if refresh_type not in {"unique"}:
        is_9_16 = bool(record.get("is_9_16", False))
        render_preview_with_hook(
            source_video=source_video,
            preview_video=preview_video,
            hook=hook,
            music_track=music_track,
            force_9_16=is_9_16,
        )
        # Re-apply uniqueization after re-rendering
        # render_unique_preview(preview_video)

    record["hook"] = hook
    record["description"] = description
    record["caption"] = caption
    record["music_track"] = str(music_track) if music_track else None
    record["status"] = "preview_ready"
    if 'generated' in locals() and isinstance(generated, dict):
        record["prompt_source"] = generated.get("prompt_source")

    _persist_pending_preview(state, record)
    resp = _build_preview_response(record)
    if 'generated' in locals() and isinstance(generated, dict):
        resp.prompt_source = generated.get("prompt_source")
    return resp
