module PipelinesNav exposing (..)

import Html exposing (Html)
import Html.Attributes exposing (class, href, id, disabled, attribute, style)
import Html.Events as Events
import Http
import Json.Decode
import List
import Mouse exposing (Position)
import Task

import Concourse.Pipeline exposing (Pipeline, fetchPipelines)

type alias Model =
  { teams : Maybe (List (String, List UIPipeline))
  , dragInfo : Maybe DragInfo
  }

purposefulThreshold : Int
purposefulThreshold = 10 -- in pixels

type alias DragInfo =
  { startPos : Position
  , pos : Position
  , teamName : String
  , pipelineName : String
  , purposeful : Bool
  , hover : Maybe (String, ListHover String)
  }

type alias UIPipeline =
  { pipeline : Pipeline
  , pausedChanging : Bool
  , pauseErrored : Bool
  }

type ListHover a
  = BeforeAll
  | AfterElement a

type Action
  = Noop
  | PausePipeline String String
  | UnpausePipeline String String
  | PipelinesFetched (Result Http.Error (List Pipeline))
  | PipelinePaused String String (Result Http.Error ())
  | PipelineUnpaused String String (Result Http.Error ())
  | StartDragging String String Position
  | StopDragging Position
  | Drag Position
  | Hover String (ListHover String)
  | Unhover String (ListHover String)

init : (Model, Cmd Action)
init =
  ( { teams = Nothing
    , dragInfo = Nothing
    }
  , fetchPipelines
  )

update : Action -> Model -> (Model, Cmd Action)
update action model =
  case action of
    Noop -> (model, Cmd.none)
    PausePipeline teamName pipelineName ->
      ( mapModelPipelines updatePausedChanging teamName pipelineName model
      , pausePipeline teamName pipelineName
      )
    UnpausePipeline teamName pipelineName ->
      ( mapModelPipelines updatePausedChanging teamName pipelineName model
      , unpausePipeline teamName pipelineName
      )
    PipelinesFetched (Ok pipelines)->
      ({ model | teams = Just <| groupPipelinesByTeam pipelines }, Cmd.none)
    PipelinesFetched (Err err) ->
      Debug.log ("failed to fetch pipelines: " ++ toString err) (model, Cmd.none)
    PipelinePaused teamName pipelineName (Ok ()) ->
      ( mapModelPipelines (setPaused True) teamName pipelineName model
      , Cmd.none
      )
    PipelinePaused teamName pipelineName (Err err) ->
      Debug.log
        ("failed to pause pipeline: " ++ toString err)
        ( mapModelPipelines updatePauseErrored teamName pipelineName model
        , Cmd.none
        )
    PipelineUnpaused teamName pipelineName (Ok ()) ->
      ( mapModelPipelines (setPaused False) teamName pipelineName model
      , Cmd.none
      )
    PipelineUnpaused teamName pipelineName (Err err) ->
      Debug.log
        ("failed to unpause pipeline: " ++ toString err)
        ( mapModelPipelines updatePauseErrored teamName pipelineName model
        , Cmd.none
        )
    Drag pos ->
      case model.dragInfo of
        Just dragInfo ->
          ( { model
            | dragInfo =
                Just <|
                  let newDragInfo = { dragInfo | pos = pos } in
                    { newDragInfo
                    | purposeful =
                        dragInfo.purposeful ||
                          abs (dragX newDragInfo) >= purposefulThreshold ||
                          abs (dragY newDragInfo) >= purposefulThreshold
                    }
            }
          , Cmd.none
          )
        Nothing -> (model, Cmd.none)
    StartDragging teamName pipelineName pos ->
      always
        ( { model
          | dragInfo =
              Just
                { startPos = pos
                , pos = pos
                , teamName = teamName
                , pipelineName = pipelineName
                , purposeful = False
                , hover = Nothing
                }
          }
        , Cmd.none
        ) <|
        Debug.log "start" pos
    StopDragging pos ->
      always
        ( { model | dragInfo = Nothing }
        , Cmd.none
        ) <|
        Debug.log "done" pos
    Hover teamName listHover ->
      case model.dragInfo of
        Just dragInfo ->
          if dragInfo.teamName == teamName &&
            listHover /= AfterElement dragInfo.pipelineName &&
            getPrevHover model /= Just listHover then
            ( { model
              | dragInfo = Just { dragInfo | hover = Just (teamName, listHover) }
              }
            , Cmd.none
            )
          else (model, Cmd.none)
        Nothing -> (model, Cmd.none)
    Unhover teamName listHover ->
      case model.dragInfo of
        Just dragInfo ->
          if dragInfo.hover == Just (teamName, listHover) then
            ( { model
              | dragInfo = Just { dragInfo | hover = Nothing }
              }
            , Cmd.none
            )
          else (model, Cmd.none)
        Nothing -> (model, Cmd.none)

getPrevHover : Model -> Maybe (ListHover String)
getPrevHover model =
  case model.dragInfo of
    Just dragInfo ->
      Maybe.oneOf <|
        List.map
          (getPrevHoverForTeam dragInfo.teamName dragInfo.pipelineName) <|
          Maybe.withDefault [] model.teams
    Nothing -> Nothing

getPrevHoverForTeam : String -> String -> (String, List UIPipeline) -> Maybe (ListHover String)
getPrevHoverForTeam teamName pipelineName (actualTeamName, pipelines) =
  if actualTeamName == teamName then
    getPrevHoverForPipelines pipelineName pipelines
  else Nothing

getPrevHoverForPipelines : String -> List UIPipeline -> Maybe (ListHover String)
getPrevHoverForPipelines pipelineName pipelines =
  case pipelines of
    [] ->
      Nothing
    [ first ] ->
      Just BeforeAll
    first :: second :: rest ->
      if second.pipeline.name == pipelineName then
        Just <| AfterElement first.pipeline.name
      else
        getPrevHoverForPipelines pipelineName <| second :: rest


dragX : DragInfo -> Int
dragX dragInfo = dragInfo.pos.x - dragInfo.startPos.x

dragY : DragInfo -> Int
dragY dragInfo = dragInfo.pos.y - dragInfo.startPos.y

isDragging : Model -> Bool
isDragging model = model.dragInfo /= Nothing

isPurposeful : Maybe DragInfo -> Bool
isPurposeful = Maybe.withDefault False << Maybe.map .purposeful

setPaused : Bool -> UIPipeline -> UIPipeline
setPaused paused uip =
  -- arbitrary elm restriction: record update syntax only works on local variables
  let pipeline = uip.pipeline in
    { uip | pipeline = { pipeline | paused = paused }, pausedChanging = False }

mapModelPipelines : (UIPipeline -> UIPipeline) -> String -> String -> Model -> Model
mapModelPipelines f teamName pipelineName model =
  { model
  | teams =
      Maybe.map
        (List.map <| mapTeamPipelines f teamName pipelineName)
        model.teams
  }

updatePausedChanging : UIPipeline -> UIPipeline
updatePausedChanging uip = {uip | pausedChanging = True, pauseErrored = False}

updatePauseErrored : UIPipeline -> UIPipeline
updatePauseErrored uip = {uip | pauseErrored = True, pausedChanging = False}

mapTeamPipelines :
  (UIPipeline -> UIPipeline) -> String -> String -> (String, List UIPipeline) -> (String, List UIPipeline)
mapTeamPipelines f teamName pipelineName (actualTeamName, pipelines) =
  ( actualTeamName
  , if actualTeamName == teamName then
      List.map (mapPipeline f pipelineName) pipelines
    else pipelines
  )

mapPipeline : (UIPipeline -> UIPipeline) -> String -> UIPipeline -> UIPipeline
mapPipeline f pipelineName uip =
  if uip.pipeline.name == pipelineName then f uip
  else uip

view : Model -> Html Action
view model =
  Html.div [ class "sidebar js-sidebar" ]
    [ case model.teams of
        Nothing -> Html.text "loading"
        Just teams ->
          Html.ul [] <| List.map (viewTeam model.dragInfo) teams
    ]

viewTeam : Maybe DragInfo -> (String, List UIPipeline) -> Html Action
viewTeam maybeDragInfo (teamName, pipelines) =
  Html.li [class "team"]
    [ Html.text teamName
    , Html.ul [] <|
      let firstElem = List.head pipelines
        in
          case firstElem of
            Nothing -> []
            Just firstElem ->
              let
                firstElemView = viewFirstPipeline maybeDragInfo teamName firstElem
              in let
                restView =
                  List.map
                    (viewPipeline maybeDragInfo teamName) <|
                      Maybe.withDefault [] <| List.tail pipelines
              in
                firstElemView :: restView
    ]

viewFirstPipeline : Maybe DragInfo -> String -> UIPipeline -> Html Action
viewFirstPipeline maybeDragInfo teamName uip =
  Html.li
    ( case maybeDragInfo of
        Just dragInfo ->
          case dragInfo.hover of
            Just hover ->
              if hover == (teamName, BeforeAll) then
                [ class "space-before" ]
              else if hover == (teamName, AfterElement uip.pipeline.name) then
                [ class "space-after" ]
              else
                []
            Nothing -> []
        Nothing -> []
    ) <|
    ( if isPurposeful maybeDragInfo then
        [ viewFirstDropArea teamName uip.pipeline.name
        , viewDropArea teamName uip.pipeline.name
        ]
      else []
    ) ++
      [ viewDraggable maybeDragInfo teamName uip ]

viewPipeline : Maybe DragInfo -> String -> UIPipeline -> Html Action
viewPipeline maybeDragInfo teamName uip =
  Html.li
    ( case maybeDragInfo of
        Just dragInfo ->
          case dragInfo.hover of
            Just hover ->
              if hover == (teamName, AfterElement uip.pipeline.name) then
                [ class "space-after" ]
              else
                []
            Nothing -> []
        Nothing -> []
    )<|
    ( if isPurposeful maybeDragInfo then
        [ viewDropArea teamName uip.pipeline.name ]
      else []
    ) ++
      [ viewDraggable maybeDragInfo teamName uip ]

viewDraggable : Maybe DragInfo -> String -> UIPipeline -> Html Action
viewDraggable maybeDragInfo teamName uip =
  Html.div
    ( [ class "draggable"
      , Events.onWithOptions
          "mousedown"
          { stopPropagation = False, preventDefault = True } <|
          Json.Decode.map (StartDragging teamName uip.pipeline.name) Mouse.position
      ] ++
        case maybeDragInfo of
          Just dragInfo ->
            if dragInfo.teamName == teamName && dragInfo.pipelineName == uip.pipeline.name then
              [ dragStyle dragInfo ]
            else []
          Nothing -> []
    )
    [ viewPauseButton uip
    , Html.a
        ( [ href uip.pipeline.url ] ++
          if isPurposeful maybeDragInfo then
            [ ignoreClicks ]
          else
            []
        )
        [ Html.text uip.pipeline.name ]
    ]

dragStyle : DragInfo -> Html.Attribute action
dragStyle dragInfo =
  style
    [ ("left", toString (dragX dragInfo) ++ "px")
    , ("top", toString (dragY dragInfo) ++ "px")
    ]

viewFirstDropArea : String -> String -> Html Action
viewFirstDropArea teamName pipelineName =
  Html.div
    [ class "drop-area first"
    , Events.onMouseEnter <| Hover teamName BeforeAll
    , Events.onMouseLeave <| Unhover teamName BeforeAll
    ]
    []

viewDropArea : String -> String -> Html Action
viewDropArea teamName pipelineName =
  Html.div
    [ class "drop-area"
    , Events.onMouseEnter <| Hover teamName <| AfterElement pipelineName
    , Events.onMouseLeave <| Unhover teamName <| AfterElement pipelineName
    ]
    []

ignoreClicks : Html.Attribute Action
ignoreClicks =
  Events.onWithOptions
    "click"
    { stopPropagation = False, preventDefault = True } <|
    Json.Decode.succeed Noop

viewPauseButton : UIPipeline -> Html Action
viewPauseButton uip =
  if uip.pipeline.paused then
    Html.span
      [ Events.onClick <| UnpausePipeline uip.pipeline.teamName uip.pipeline.name
      , Html.Attributes.style
          [ ("backgroundColor"
            , if uip.pauseErrored then "orange" else "blue"
            )
          ]
      ] <|
      if uip.pausedChanging then
        [ Html.i [class "fa fa-fw fa-circle-o-notch fa-spin"] [] ]
      else
        [ Html.i [class "fa fa-fw fa-play"] [] ]
  else
    Html.span
      [ Events.onClick <| PausePipeline uip.pipeline.teamName uip.pipeline.name
      , Html.Attributes.style
          [ ("backgroundColor"
            , if uip.pauseErrored then "orange" else "blue"
            )
          ]
      ] <|
      if uip.pausedChanging then
        [ Html.i [class "fa fa-fw fa-circle-o-notch fa-spin"] [] ]
      else
        [ Html.i [class "fa fa-fw fa-pause"] [] ]

fetchPipelines : Cmd Action
fetchPipelines =
  Cmd.map PipelinesFetched <|
    Task.perform Err Ok Concourse.Pipeline.fetchPipelines

unpausePipeline : String -> String -> Cmd Action
unpausePipeline teamName pipelineName =
  Cmd.map (PipelineUnpaused teamName pipelineName) <|
    Task.perform Err Ok <| Concourse.Pipeline.unpause teamName pipelineName

pausePipeline : String -> String -> Cmd Action
pausePipeline teamName pipelineName =
  Cmd.map (PipelinePaused teamName pipelineName) <|
    Task.perform Err Ok <| Concourse.Pipeline.pause teamName pipelineName

groupPipelinesByTeam : List Pipeline -> List (String, List UIPipeline)
groupPipelinesByTeam pipelines =
  let
    firstPipeline = List.head pipelines
  in
    case firstPipeline of
      Nothing -> []
      Just firstPipeline ->
        let
          (teamGroup, rest) = List.partition (\p -> p.teamName == firstPipeline.teamName) pipelines
        in let
          team = List.map toUIPipeline teamGroup
        in
          (firstPipeline.teamName, team) :: (groupPipelinesByTeam rest)

toUIPipeline : Pipeline -> UIPipeline
toUIPipeline pipeline =
  { pipeline = pipeline
  , pausedChanging = False
  , pauseErrored = False
  }
