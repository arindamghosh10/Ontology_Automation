"""
Pipeline orchestrator.
FIX 1: Always writes ALL data to Sheet1 regardless of confidence score.
FIX 2: Confidence score only determines whether REVIEW entry is added.
FIX 3: Excel file lock detection with clear error message.
"""

import asyncio
import logging
import os
import time

from .config import Config
from .excel_handler import (
    read_merchants, write_merchant_result, write_location_rows,
    ensure_review_sheet, write_review_entry,
)
from .llm_client import LLMClient
from .scraper import SmartScraper
from .search_engine import SearchEngine
from .validators import calculate_confidence

from .steps import (
    step1_website, step2_zoominfo, step3_dnb, step4_wikipedia,
    step5_acquisitions, step6_phone, step7_locations,
)

logger = logging.getLogger(__name__)


def _check_file_accessible(filepath: str) -> bool:
    """Check if Excel file is writable (not open in Excel)."""
    try:
        with open(filepath, 'r+b'):
            return True
    except PermissionError:
        return False
    except Exception:
        return True


def _validate_merchant_input(merchant: dict) -> list:
    issues = []
    store_domain = merchant.get("store_domain", "")
    store_name = merchant.get("store_name", "")
    if not store_name or str(store_name).strip().lower() in ("", "nan", "none"):
        issues.append("Missing store_name")
    if not store_domain or str(store_domain).strip().lower() in ("", "nan", "none", "n/a"):
        issues.append("Missing or invalid store_domain")
    return issues


async def _run_step_with_retry(step_func, step_name, merchant, config, **kwargs):
    store_name = merchant.get("store_name", "Unknown")
    for attempt in range(1, config.pipeline.retry_attempts + 1):
        try:
            return await step_func(merchant=merchant, **kwargs)
        except Exception as e:
            logger.error(f"[{store_name}] {step_name} attempt {attempt} failed: {e}")
            if attempt < config.pipeline.retry_attempts:
                await asyncio.sleep(config.pipeline.retry_delay)
            else:
                return {"_error": str(e), "_step": step_name}
    return {"_error": "Unknown", "_step": step_name}


async def process_merchant(merchant, config, scraper, search, llm, filepath):
    store_name = merchant.get("store_name", "Unknown")
    store_id = merchant.get("store_id", "")
    row = merchant.get("_row", 0)

    logger.info("=" * 60)
    logger.info(f"Processing: {store_name} (ID: {store_id}, Row: {row})")
    logger.info("=" * 60)

    # Input validation
    input_issues = _validate_merchant_input(merchant)
    if input_issues:
        issue_str = "; ".join(input_issues)
        logger.warning(f"[{store_name}] Skipping — {issue_str}")
        write_merchant_result(filepath, row, {
            "verification_notes": f"SKIPPED: {issue_str}",
            "Confidence_Score": 0,
        })
        write_review_entry(filepath, {
            "store_id": store_id, "store_name": store_name,
            "field_name": "store_domain",
            "candidate_value": str(merchant.get("store_domain", "")),
            "reason_flagged": f"Invalid input: {issue_str}",
            "confidence_score": 0, "recommended_action": "Manual lookup",
        })
        return {"store_id": store_id, "store_name": store_name,
                "confidence": 0, "skipped": True, "reason": issue_str}

    context = {}
    all_notes = []

    # Industry detection
    try:
        industry = await llm.analyze_industry(store_name, merchant.get("store_domain", ""), "")
        context["industry"] = industry
        logger.info(f"[{store_name}] Industry: {industry}")
    except Exception as e:
        logger.warning(f"[{store_name}] Industry detection failed: {e}")
        context["industry"] = ""

    # Run all 7 steps
    for step_name, step_func, extra_kwargs in [
        ("Step 1 (Website)", step1_website.execute, {"scraper": scraper}),
        ("Step 2 (ZoomInfo)", step2_zoominfo.execute, {"scraper": scraper, "search": search, "llm": llm}),
        ("Step 3 (D&B)", step3_dnb.execute, {"scraper": scraper, "search": search, "llm": llm}),
        ("Step 4 (Wikipedia)", step4_wikipedia.execute, {"scraper": scraper, "search": search, "llm": llm}),
        ("Step 5 (Acquisitions)", step5_acquisitions.execute, {"search": search, "llm": llm}),
        ("Step 6 (Phone)", step6_phone.execute, {"search": search}),
        ("Step 7 (Locations)", step7_locations.execute, {"scraper": scraper, "search": search, "llm": llm}),
    ]:
        step_result = await _run_step_with_retry(
            step_func, step_name, merchant, config,
            context=context, **extra_kwargs
        )
        context.update({k: v for k, v in step_result.items() if not k.startswith("_")})
        if step_result.get("verification_notes"):
            all_notes.append(step_result["verification_notes"])

    # Calculate confidence
    scores = {
        "website_score": context.get("website_score", 0),
        "zoominfo_score": context.get("zoominfo_score", 0),
        "dnb_score": context.get("dnb_score", 0),
        "wikipedia_score": context.get("wikipedia_score", 0),
        "acquisitions_score": context.get("acquisitions_score", 0),
        "phone_score": context.get("phone_score", 0),
        "location_score": context.get("location_score", 0),
    }
    confidence = calculate_confidence(scores)
    logger.info(f"[{store_name}] Confidence: {confidence}/100 | {scores}")

    for key in ["zoominfo_failures", "dnb_failures", "wikipedia_failures"]:
        failures = context.get(key, [])
        if failures:
            all_notes.extend(failures[:2])

    verification_notes = "; ".join(all_notes) if all_notes else ""

    write_data = {
        "corrected_website_text": context.get("corrected_website_text", ""),
        "corrected_merchant_zoominfo_url": context.get("corrected_merchant_zoominfo_url", ""),
        "corrected_zoominfo_page_text": context.get("corrected_zoominfo_page_text", ""),
        "corrected_merchant_dnb_url": context.get("corrected_merchant_dnb_url", ""),
        "corrected_dnb_page_text": context.get("corrected_dnb_page_text", ""),
        "corrected_merchant_wikipedia_url": context.get("corrected_merchant_wikipedia_url", ""),
        "corrected_wikipedia_page_text": context.get("corrected_wikipedia_page_text", ""),
        "acquisitions": context.get("acquisitions", ""),
        "other_phone_numbers": context.get("other_phone_numbers", ""),
        "verification_notes": verification_notes,
        "Confidence_Score": confidence,
    }

    # FIX: ALWAYS write all data to Sheet1. Score only controls REVIEW.
    try:
        write_merchant_result(filepath, row, write_data)

        if confidence >= config.pipeline.confidence_auto_write:
            logger.info(f"[{store_name}] -> Sheet1 (score {confidence} >= 85, no review needed)")

        elif confidence >= config.pipeline.confidence_review_write:
            logger.info(f"[{store_name}] -> Sheet1 + REVIEW flag (score {confidence})")
            write_review_entry(filepath, {
                "store_id": store_id, "store_name": store_name,
                "field_name": "overall",
                "candidate_value": f"score={confidence}",
                "reason_flagged": f"Mid confidence ({confidence}): verify key fields",
                "confidence_score": confidence,
                "recommended_action": "Spot-check URLs and text",
            })

        else:
            logger.info(f"[{store_name}] -> Sheet1 + REVIEW (low score {confidence})")
            write_review_entry(filepath, {
                "store_id": store_id, "store_name": store_name,
                "field_name": "overall",
                "candidate_value": f"score={confidence}",
                "reason_flagged": f"Low confidence ({confidence}): all fields need manual review",
                "confidence_score": confidence,
                "recommended_action": "Manual review required",
            })

        locations = context.get("locations", [])
        if locations:
            write_location_rows(filepath, store_name, str(store_id), locations)
            logger.info(f"[{store_name}] {len(locations)} locations written")

    except PermissionError:
        logger.error(
            f"[{store_name}] PERMISSION DENIED writing to {filepath}\n"
            f"  >> Close the Excel file in Microsoft Excel and rerun. <<"
        )
    except Exception as e:
        logger.error(f"[{store_name}] Excel write error: {e}")

    return {"store_id": store_id, "store_name": store_name,
            "confidence": confidence, "scores": scores}


async def process_batch(merchants, config, scraper, search, llm, filepath):
    results = []
    for i, merchant in enumerate(merchants, 1):
        store_name = merchant.get("store_name", "Unknown")
        logger.info(f"\n{'#'*60}\nBatch {i}/{len(merchants)} — {store_name}\n{'#'*60}\n")
        try:
            result = await process_merchant(merchant, config, scraper, search, llm, filepath)
            results.append(result)
        except Exception as e:
            logger.error(f"[{store_name}] Fatal: {e}")
            results.append({"store_id": merchant.get("store_id", ""),
                            "store_name": store_name, "confidence": 0, "error": str(e)})
    return results


async def run_pipeline(filepath, config, start_row=0, sheet_name="Sheet1"):
    start_time = time.time()

    # FIX: Check file accessibility before starting
    if not _check_file_accessible(filepath):
        raise RuntimeError(
            f"\n\nERROR: Cannot write to {filepath}\n"
            f"The Excel file appears to be open in Microsoft Excel.\n"
            f"Please CLOSE the file in Excel and run again.\n"
        )

    logger.info(f"Starting pipeline | {filepath} | Batch: {config.pipeline.batch_size}")
    for w in config.validate():
        logger.warning(f"Config: {w}")

    ensure_review_sheet(filepath)
    merchants = read_merchants(filepath, sheet_name)
    if start_row > 0:
        merchants = [m for m in merchants if m.get("_row", 0) >= start_row]

    total = len(merchants)
    logger.info(f"Found {total} merchants")
    if total == 0:
        return {"total": 0, "message": "No merchants found"}

    scraper = SmartScraper(config.scraper, config.firecrawl, config.proxy, config.captcha)
    search = SearchEngine(config.search, config.scraper, config.proxy, config.serpapi)
    llm = LLMClient(config.llm)
    all_results = []
    failure_reasons = {}

    try:
        for batch_start in range(0, total, config.pipeline.batch_size):
            batch_end = min(batch_start + config.pipeline.batch_size, total)
            batch = merchants[batch_start:batch_end]
            batch_num = (batch_start // config.pipeline.batch_size) + 1
            total_batches = (total + config.pipeline.batch_size - 1) // config.pipeline.batch_size
            logger.info(f"\n{'='*60}\nBATCH {batch_num}/{total_batches}\n{'='*60}")
            batch_results = await process_batch(batch, config, scraper, search, llm, filepath)
            all_results.extend(batch_results)
            for r in batch_results:
                if r.get("confidence", 0) < 60:
                    reason = r.get("error", r.get("reason", "Low confidence"))
                    failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
    finally:
        await scraper.close()
        await search.close()
        await llm.close()

    elapsed = time.time() - start_time
    auto_written = sum(1 for r in all_results if r.get("confidence", 0) >= 85)
    flagged = sum(1 for r in all_results if 60 <= r.get("confidence", 0) < 85)
    failed = sum(1 for r in all_results if r.get("confidence", 0) < 60)
    skipped = sum(1 for r in all_results if r.get("skipped"))
    most_common = max(failure_reasons, key=failure_reasons.get) if failure_reasons else "N/A"

    summary = {
        "total_merchants": total, "auto_written": auto_written,
        "flagged_for_review": flagged, "failed": failed, "skipped": skipped,
        "most_common_failure": most_common,
        "elapsed_formatted": f"{int(elapsed//60)}m {int(elapsed%60)}s",
    }

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Total:              {total}")
    print(f"Skipped (no domain):{skipped}")
    print(f"High confidence:    {auto_written}  (score >= 85, auto-written)")
    print(f"Mid confidence:     {flagged}  (score 60-84, written + flagged)")
    print(f"Low confidence:     {failed}  (score < 60, written + needs review)")
    print(f"Time:               {summary['elapsed_formatted']}")
    print("=" * 60)
    print("NOTE: ALL merchants now written to Sheet1 regardless of score.")
    print("      REVIEW sheet shows items needing human verification.")
    return summary
