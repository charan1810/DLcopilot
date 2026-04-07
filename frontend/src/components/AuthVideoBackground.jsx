import { useEffect, useRef, useState } from "react";

const LOOP_START_RATIO = 0.35;
const SEAM_WINDOW_SECONDS = 1.05;
const RETURN_WINDOW_SECONDS = 0.8;
const SECONDARY_OFFSET_SECONDS = 0.2;

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function safePlay(video) {
    if (!video) return;
    const playPromise = video.play();
    if (playPromise && typeof playPromise.catch === "function") {
        playPromise.catch(() => {
            // Ignore autoplay rejections; user interaction will resume playback.
        });
    }
}

export default function AuthVideoBackground({ src }) {
    const primaryRef = useRef(null);
    const secondaryRef = useRef(null);
    const durationRef = useRef(0);
    const loopStartRef = useRef(0);
    const showingSecondaryRef = useRef(false);

    const [showSecondary, setShowSecondary] = useState(false);

    useEffect(() => {
        showingSecondaryRef.current = showSecondary;
    }, [showSecondary]);

    useEffect(() => {
        const primary = primaryRef.current;
        const secondary = secondaryRef.current;

        if (!primary || !secondary) return;

        setShowSecondary(false);

        const restartIntoLoop = (video, offsetSeconds = 0) => {
            const duration = durationRef.current || Number(video.duration) || 0;
            if (!duration) return;

            const loopStart = loopStartRef.current;
            const maxSeek = Math.max(duration - 0.15, loopStart);
            const nextTime = clamp(loopStart + offsetSeconds, loopStart, maxSeek);
            video.currentTime = nextTime;
            safePlay(video);
        };

        const handleLoadedMetadata = () => {
            const duration = Number(primary.duration) || 0;
            durationRef.current = duration;

            if (duration > 0) {
                const loopStart = clamp(duration * LOOP_START_RATIO, 0, Math.max(duration - 1.4, 0));
                loopStartRef.current = loopStart;

                primary.currentTime = loopStart;
                secondary.currentTime = clamp(loopStart + SECONDARY_OFFSET_SECONDS, loopStart, Math.max(duration - 0.2, loopStart));
            }

            safePlay(primary);
            safePlay(secondary);
        };

        const handlePrimaryTimeUpdate = () => {
            const duration = durationRef.current || Number(primary.duration) || 0;
            if (!duration) return;

            const t = Number(primary.currentTime) || 0;
            const loopStart = loopStartRef.current;

            if (!showingSecondaryRef.current && t >= Math.max(loopStart, duration - SEAM_WINDOW_SECONDS)) {
                restartIntoLoop(secondary, SECONDARY_OFFSET_SECONDS);
                setShowSecondary(true);
                return;
            }

            if (showingSecondaryRef.current && t <= loopStart + RETURN_WINDOW_SECONDS) {
                setShowSecondary(false);
            }
        };

        const handlePrimaryEnded = () => {
            restartIntoLoop(primary, 0);
        };

        const handleSecondaryEnded = () => {
            restartIntoLoop(secondary, SECONDARY_OFFSET_SECONDS);
        };

        primary.addEventListener("loadedmetadata", handleLoadedMetadata);
        primary.addEventListener("timeupdate", handlePrimaryTimeUpdate);
        primary.addEventListener("ended", handlePrimaryEnded);
        secondary.addEventListener("ended", handleSecondaryEnded);

        safePlay(primary);
        safePlay(secondary);

        return () => {
            primary.removeEventListener("loadedmetadata", handleLoadedMetadata);
            primary.removeEventListener("timeupdate", handlePrimaryTimeUpdate);
            primary.removeEventListener("ended", handlePrimaryEnded);
            secondary.removeEventListener("ended", handleSecondaryEnded);
        };
    }, [src]);

    return (
        <div className="auth-video-wrap" aria-hidden="true">
            <video
                ref={primaryRef}
                className={`auth-bg-video auth-bg-video-primary ${showSecondary ? "is-hidden" : "is-active"}`}
                autoPlay
                muted
                playsInline
                preload="auto"
            >
                <source src={src} type="video/mp4" />
            </video>
            <video
                ref={secondaryRef}
                className={`auth-bg-video auth-bg-video-secondary ${showSecondary ? "is-active" : "is-hidden"}`}
                autoPlay
                muted
                playsInline
                preload="auto"
            >
                <source src={src} type="video/mp4" />
            </video>
            <div className="auth-video-overlay" />
        </div>
    );
}
