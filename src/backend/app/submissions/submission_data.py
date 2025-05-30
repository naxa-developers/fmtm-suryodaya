import asyncio
import json

from fastapi import Response
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Any
from loguru import logger as log
from osm_fieldwork.OdkCentral import OdkForm

from app.central import central_crud
from app.central.central_crud import get_odk_form
from app.db.enums import HTTPStatus
from app.db.models import DbProject

async def _handle_csv_download(
        project: DbProject,
        filters: Dict[str, str]
    ) -> Response:
    """Handle CSV download in zip."""
    file_content = await gather_all_submission_csvs(project, filters)
    headers = {
        "Content-Disposition": f"attachment; filename={project.slug}.zip",
        "Content-Type": "application/zip"
    }
    return Response(file_content, headers=headers)


async def _handle_json_download(
        project: DbProject,
        filters: Dict[str, str]
    ) -> Response:
    """Handle JSON download with streaming response."""
    return await download_submission_in_json(project, filters, geojson=False)


async def _handle_geojson_download(
        project: DbProject,
        filters: Dict[str, str]
    ) -> Any:
    """Handle GeoJSON download."""
    data = await download_submission_in_json(project, filters, geojson=True)
    submission_json = data.get("value", []) if data else []
    
    return await central_crud.convert_odk_submission_json_to_geojson(
        submission_json, project
    )


async def gather_all_submission_csvs(
        project: DbProject,
        filters: Dict[str, str]
    ) -> bytes:
    """Gather all submission CSVs."""
    log.info(f"Downloading CSV submissions for project {project.id}")
    
    try:
        xform = get_odk_form(project.odk_credentials)
        file_response = xform.getSubmissionMedia(
            project.odkid,
            project.odk_form_id,
            filters
        )
        return file_response.content
    except Exception as e:
        log.error(f"Failed to download CSV for project {project.id}: {str(e)}")
        raise


async def download_submission_in_json(
    project: DbProject,
    filters: Dict[str, str],
    geojson: bool = False
) -> Optional[Response]:
    """Download and process submission data."""
    try:
        xform = get_odk_form(project.odk_credentials)
        
        # Fetch base submission data
        data = xform.listSubmissions(project.odkid, project.odk_form_id, filters)
        base_submissions = data.get("value", [])
        
        if not base_submissions:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail="No Submissions Found."
            )
        
        processed_submissions = await _process_submissions_batch(
            base_submissions, xform, project
        )
        
        data["value"] = processed_submissions
        
        if geojson:
            return data
        
        return _create_json_response(data, project)
        
    except Exception as e:
        log.error(f"Failed to download JSON for project {project.id}: {str(e)}")
        raise


async def _process_submissions_batch(
    submissions: List[Dict],
    xform: OdkForm,
    project: DbProject,
    batch_size: int = 50
) -> List[Dict]:
    """Process submissions in batches for better memory management."""
    processed = []
    hashtags = project.hashtags
    
    # Process in batches to control memory usage and concurrency
    for i in range(0, len(submissions), batch_size):
        batch = submissions[i:i + batch_size]
        
        # Process batch concurrently
        batch_tasks = [
            _inject_repeat_data(submission, xform, project)
            for submission in batch
        ]
        
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                log.warning(f"Failed to process submission {i+j}: {str(result)}")
                # Use original submission if repeat injection fails
                result = batch[j].copy()
            
            result["hashtags"] = hashtags
            processed.append(result)
    
    return processed


async def _inject_repeat_data(
    submission: Dict,
    odk_form: OdkForm,
    project: DbProject
) -> Dict:
    """
    Inject repeat group data in submissions if any.
    """
    # Find repeat navigation links efficiently
    repeat_links = {
        key: submission[key]
        for key in submission
        if key.endswith("@odata.navigationLink")
    }
    
    if not repeat_links:
        return submission
    
    # Fetch all repeat data concurrently
    repeat_tasks = [
        _fetch_repeat_data(odk_form, project, repeat_name, repeat_path)
        for repeat_name, repeat_path in repeat_links.items()
    ]
    
    repeat_results = await asyncio.gather(*repeat_tasks, return_exceptions=True)
    
    new_submission = {}
    repeat_data_blocks = []
    
    for (repeat_name, _), result in zip(repeat_links.items(), repeat_results):
        if isinstance(result, Exception):
            log.warning(f"Failed to fetch repeat data for {repeat_name}: {str(result)}")
            continue
            
        group_name = repeat_name.replace("@odata.navigationLink", "")
        repeat_data_blocks.append((group_name, result))
    
    # Rebuild submission dict with single pass
    for key, val in submission.items():
        if key.endswith("@odata.navigationLink"):
            continue  # Skip navigation links
            
        if key == "meta":
            # Insert all repeat data before meta
            for group_name, group_data in repeat_data_blocks:
                new_submission[group_name] = group_data
        
        new_submission[key] = val
    
    return new_submission


async def _fetch_repeat_data(
    odk_form: OdkForm, 
    project: DbProject, 
    repeat_name: str, 
    repeat_path: str
) -> List[Dict]:
    """Fetch and parse repeat data with error handling."""
    try:
        raw_data = odk_form.getRepeatData(
            project.odkid,
            project.odk_form_id,
            repeat_path
        )
        
        repeat_json = json.loads(raw_data)
        return repeat_json.get("value", [])
        
    except json.JSONDecodeError as e:
        log.error(f"Invalid JSON in repeat data for {repeat_name}: {str(e)}")
        return []
    except Exception as e:
        log.error(f"Failed to fetch repeat data for {repeat_name}: {str(e)}")
        return []


# def _create_empty_response(project: DbProject, geojson: bool) -> Optional[Response]:
#     """Create response for empty submission data."""
#     empty_data = {"value": []}
    
#     if geojson:
#         return empty_data
    
#     return _create_json_response(empty_data, project)


def _create_json_response(data: Dict, project: DbProject) -> Response:
    """Create JSON response with streaming."""
    try:
        # Use separators for compact JSON and ensure_ascii=False for better performance
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        json_bytes = json_str.encode('utf-8')
        
        headers = {
            "Content-Disposition": f"attachment; filename={project.slug}_submissions.json",
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(json_bytes))
        }
        
        return Response(content=json_bytes, headers=headers)
        
    except Exception as e:
        log.error(f"Failed to create JSON response for project {project.id}: {str(e)}")
        raise


# Alternative streaming version for very large datasets
# async def _create_streaming_json_response(data: Dict, project: DbProject) -> Response:
#     """Create streaming JSON response for large datasets."""
#     async def generate():
#         yield '{"value":['
        
#         submissions = data.get("value", [])
#         for i, submission in enumerate(submissions):
#             if i > 0:
#                 yield ','
#             yield json.dumps(submission, separators=(',', ':'), ensure_ascii=False)
        
#         yield ']}'
    
#     headers = {
#         "Content-Disposition": f"attachment; filename={project.slug}_submissions.json",
#         "Content-Type": "application/json; charset=utf-8"
#     }
    
#     return StreamingResponse(generate(), headers=headers)